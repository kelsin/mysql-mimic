from __future__ import annotations

from contextlib import contextmanager
from typing import (
    Dict,
    List,
    TYPE_CHECKING,
    Optional,
    Callable,
    Awaitable,
    Type,
    Any,
    Iterator,
)

from sqlglot import Dialect
from sqlglot.dialects import MySQL
from sqlglot import expressions as exp
from sqlglot.executor import execute

from mysql_mimic.charset import CharacterSet
from mysql_mimic.errors import ErrorCode, MysqlError
from mysql_mimic.intercept import (
    setitem_kind,
    value_to_expression,
    expression_to_value,
    TRANSACTION_CHARACTERISTICS,
)
from mysql_mimic.schema import (
    show_statement_to_info_schema_query,
    like_to_regex,
    BaseInfoSchema,
    ensure_info_schema,
)
from mysql_mimic.constants import INFO_SCHEMA
from mysql_mimic.utils import find_dbs
from mysql_mimic.variables import Variables, SessionVariables, GlobalVariables, DEFAULT
from mysql_mimic.results import AllowedResult

if TYPE_CHECKING:
    from mysql_mimic.connection import Connection

    Interceptor = Callable[[exp.Expression], Awaitable[AllowedResult]]


class BaseSession:
    """
    Session interface.

    This defines what the Connection object depends on.

    Most applications to implement the abstract `Session`, not this class.
    """

    variables: Variables
    username: Optional[str]
    database: Optional[str]

    async def handle_query(self, sql: str, attrs: Dict[str, str]) -> AllowedResult:
        """
        Main entrypoint for queries.

        Args:
            sql: SQL statement
            attrs: Mapping of query attributes
        Returns:
            One of:
            - tuple(rows, column_names), where "rows" is a sequence of sequences
              and "column_names" is a sequence of strings with the same length
              as every row in "rows"
            - tuple(rows, result_columns), where "rows" is the same
              as above, and "result_columns" is a sequence of mysql_mimic.ResultColumn
              instances.
            - mysql_mimic.ResultSet instance
        """

    async def init(self, connection: Connection) -> None:
        """
        Called when connection phase is complete.
        """

    async def close(self) -> None:
        """
        Called when the client closes the connection.
        """

    async def reset(self) -> None:
        """
        Called when a client resets the connection and after a COM_CHANGE_USER command.
        """

    async def use(self, database: str) -> None:
        """
        Use a new default database.

        Called when a USE database_name command is received.

        Args:
            database: database name
        """


class Session(BaseSession):
    """
    Abstract session.

    This automatically handles lots of behavior that many clients except,
    e.g. session variables, SHOW commands, queries to INFORMATION_SCHEMA, and more
    """

    dialect: Type[Dialect] = MySQL

    def __init__(self, variables: Variables | None = None):
        self.variables = variables or SessionVariables(GlobalVariables())

        # Query interceptors.
        # If any of these return a non-None value, the query will be cut short.
        self.interceptors: list[Interceptor] = [
            self._replace_variables_interceptor,
            self._set_interceptor,
            self._static_query_interceptor,
            self._use_interceptor,
            self._show_interceptor,
            self._describe_interceptor,
            self._rollback_interceptor,
            self._info_schema_interceptor,
        ]

        # Information functions.
        # These will be replaced in the AST with their corresponding values.
        self._functions = {
            "CONNECTION_ID": lambda: self.connection.connection_id,
            "USER": lambda: self.variables.get("external_user"),
            "SYSTEM_USER": lambda: self.variables.get("external_user"),
            "SESSION_USER": lambda: self.variables.get("external_user"),
            "CURRENT_USER": lambda: self.username,
            "VERSION": lambda: self.variables.get("version"),
            "DATABASE": lambda: self.database,
            "SCHEMA": lambda: self.database,
        }

        # Current database
        self.database = None

        # Current authenticated user
        self.username = None

        self._connection: Optional[Connection] = None

    async def query(
        self, expression: exp.Expression, sql: str, attrs: Dict[str, str]
    ) -> AllowedResult:
        """
        Process a SQL query.

        Args:
            expression: parsed AST of the statement from client
            sql: original SQL statement from client
            attrs: arbitrary query attributes set by client
        Returns:
            One of:
            - tuple(rows, column_names), where "rows" is a sequence of sequences
              and "column_names" is a sequence of strings with the same length
              as every row in "rows"
            - tuple(rows, result_columns), where "rows" is the same
              as above, and "result_columns" is a sequence of mysql_mimic.ResultColumn
              instances.
            - mysql_mimic.ResultSet instance
        """
        return [], []

    async def schema(self) -> dict | BaseInfoSchema:
        """
        Provide the database schema.

        This is used to serve INFORMATION_SCHEMA and SHOW queries.

        Returns:
            One of:
            - Mapping of:
                {table: {column: column_type}} or
                {db: {table: {column: column_type}}} or
                {catalog: {db: {table: {column: column_type}}}}
            - Instance of `BaseInfoSchema`
        """
        return {}

    @property
    def connection(self) -> Connection:
        """
        Get the connection associated with this session.
        """
        if self._connection is None:
            raise AttributeError("Session is not yet bound")
        return self._connection

    async def init(self, connection: Connection) -> None:
        """
        Called when connection phase is complete.
        """
        self._connection = connection

    async def close(self) -> None:
        """
        Called when the client closes the connection.
        """
        self._connection = None

    async def handle_query(self, sql: str, attrs: Dict[str, str]) -> AllowedResult:
        result = None
        for expression in self._parse(sql):
            if not expression:
                continue
            with self._set_var_hint(expression):
                result = await self._intercept(expression, sql, attrs)
                if result is None:
                    result = await self.query(expression, sql, attrs)
        return result

    async def use(self, database: str) -> None:
        self.database = database

    def _parse(self, sql: str) -> List[exp.Expression]:
        return [e for e in self.dialect().parse(sql) if e]

    async def _query_info_schema(self, expression: exp.Expression) -> AllowedResult:
        return await ensure_info_schema(await self.schema()).query(expression)

    @contextmanager
    def _set_var_hint(self, expression: exp.Expression) -> Iterator[None]:
        """Handles any SET_VAR hints, which set system variables for a single statement"""
        hint = expression.args.get("hint")
        if not hint:
            yield
            return

        assignments = {}
        for e in hint.expressions:
            if isinstance(e, exp.Func) and e.name == "SET_VAR":
                eq = e.expressions[0]
                assignments[eq.left.name] = expression_to_value(eq.right)

        orig = {k: self.variables.get(k) for k in assignments}
        try:
            for k, v in assignments.items():
                self.variables.set(k, v)
            yield
        finally:
            for k, v in orig.items():
                self.variables.set(k, v)

    async def _intercept(
        self, expression: exp.Expression, sql: str, attrs: Dict[str, str]
    ) -> AllowedResult:
        """Run all the query interceptors."""
        for interceptor in self.interceptors:
            result = await interceptor(expression)
            if result is not None:
                return result
        return None

    async def _use_interceptor(self, expression: exp.Expression) -> AllowedResult:
        """Intercept USE statements"""
        if isinstance(expression, exp.Use):
            await self.use(expression.this.name)
            return [], []
        return None

    async def _show_interceptor(self, expression: exp.Expression) -> AllowedResult:
        """Intercept SHOW statements"""
        if isinstance(expression, exp.Show):
            kind = expression.name.upper()
            if kind == "VARIABLES":
                return self._show_variables(expression)
            if kind == "STATUS":
                return self._show_status(expression)
            if kind == "WARNINGS":
                return self._show_warnings(expression)
            if kind == "ERRORS":
                return self._show_errors(expression)
            select = show_statement_to_info_schema_query(expression, self.database)
            return await self._query_info_schema(select)
        return None

    async def _describe_interceptor(self, expression: exp.Expression) -> AllowedResult:
        """Intercept DESCRIBE statements"""
        if isinstance(expression, exp.Describe):
            name = expression.this.name
            show = self.dialect().parse(f"SHOW COLUMNS FROM {name}")[0]
            return (
                await self._show_interceptor(show)
                if isinstance(show, exp.Expression)
                else None
            )
        return None

    async def _rollback_interceptor(self, expression: exp.Expression) -> AllowedResult:
        """Intercept ROLLBACK statements"""
        if isinstance(expression, exp.Rollback):
            return [], []
        return None

    async def _replace_variables_interceptor(self, expression: exp.Expression) -> None:
        """Replace session variables and information functions with their corresponding values"""

        def _transform(node: exp.Expression) -> exp.Expression:
            new_node = None

            if isinstance(node, exp.Func):
                if isinstance(node, exp.Anonymous):
                    func_name = node.name.upper()
                else:
                    func_name = node.sql_name()
                func = self._functions.get(func_name)
                if func:
                    value = func()
                    new_node = value_to_expression(value)
            elif isinstance(node, exp.Column) and node.sql() == "CURRENT_USER":
                value = self._functions["CURRENT_USER"]()
                new_node = value_to_expression(value)
            elif isinstance(node, exp.SessionParameter):
                value = self.variables.get(node.name)
                new_node = value_to_expression(value)

            if (
                new_node
                and isinstance(node.parent, exp.Select)
                and node.arg_key == "expressions"
            ):
                new_node = exp.alias_(new_node, exp.to_identifier(node.sql()))

            return new_node or node

        if isinstance(expression, exp.Set):
            for setitem in expression.expressions:
                if isinstance(setitem.this, exp.Binary):
                    # In the case of statements like: SET @@foo = @@bar
                    # We only want to replace variables on the right
                    setitem.this.set(
                        "expression",
                        setitem.this.expression.transform(_transform, copy=True),
                    )
        else:
            expression.transform(_transform, copy=False)

    async def _static_query_interceptor(
        self, expression: exp.Expression
    ) -> AllowedResult:
        """
        Handle static queries (e.g. SELECT 1).

        These very common, as many clients execute commands like SELECT DATABASE() when connecting.
        """
        if isinstance(expression, exp.Select) and not any(
            expression.args.get(a)
            for a in set(exp.Select.arg_types) - {"expressions", "limit", "hint"}
        ):
            result = execute(expression)
            return result.rows, result.columns
        return None

    async def _set_interceptor(self, expression: exp.Expression) -> AllowedResult:
        """Intercept SET statements"""
        if isinstance(expression, exp.Set):
            expressions = expression.expressions
            for item in expressions:
                assert isinstance(item, exp.SetItem)

                kind = setitem_kind(item)

                if kind == "VARIABLE":
                    self._set_variable(item)
                elif kind == "CHARACTER SET":
                    self._set_charset(item)
                elif kind == "NAMES":
                    self._set_names(item)
                elif kind == "TRANSACTION":
                    self._set_transaction(item)
                else:
                    raise MysqlError(
                        f"Unsupported SET statement: {kind}",
                        code=ErrorCode.NOT_SUPPORTED_YET,
                    )

            return [], []
        return None

    async def _info_schema_interceptor(
        self, expression: exp.Expression
    ) -> AllowedResult:
        """Intercept queries to INFORMATION_SCHEMA tables"""
        dbs = find_dbs(expression)
        if (self.database and self.database.lower() in INFO_SCHEMA) or (
            dbs and all(db.lower() in INFO_SCHEMA for db in dbs)
        ):
            return await self._query_info_schema(expression)
        return None

    def _set_variable(self, setitem: exp.SetItem) -> None:
        assignment = setitem.this
        left = assignment.left

        if isinstance(left, exp.SessionParameter):
            scope = left.text("kind") or "SESSION"
            name = left.name
        elif isinstance(left, exp.Parameter):
            raise MysqlError(
                "User-defined variables not supported yet",
                code=ErrorCode.NOT_SUPPORTED_YET,
            )
        else:
            scope = setitem.text("kind") or "SESSION"
            name = left.name

        scope = scope.upper()
        value = expression_to_value(assignment.right)

        if scope in {"SESSION", "LOCAL"}:
            self.variables.set(name, value)
        else:
            raise MysqlError(
                f"Cannot SET variable {name} with scope {scope}",
                code=ErrorCode.NOT_SUPPORTED_YET,
            )

    def _set_charset(self, item: exp.SetItem) -> None:
        charset_name: Any
        charset_conn: Any
        if item.name == "DEFAULT":
            charset_name = DEFAULT
            charset_conn = DEFAULT
        else:
            charset_name = item.name
            charset_conn = self.variables.get("character_set_database")
        self.variables.set("character_set_client", charset_name)
        self.variables.set("character_set_results", charset_name)
        self.variables.set("character_set_connection", charset_conn)

    def _set_names(self, item: exp.SetItem) -> None:
        charset_name: Any
        collation_name: Any
        if item.name == "DEFAULT":
            charset_name = DEFAULT
            collation_name = DEFAULT
        else:
            charset_name = item.name
            collation_name = (
                item.text("collate")
                or CharacterSet[charset_name].default_collation.name
            )
        self.variables.set("character_set_client", charset_name)
        self.variables.set("character_set_connection", charset_name)
        self.variables.set("character_set_results", charset_name)
        self.variables.set("collation_connection", collation_name)

    def _set_transaction(self, item: exp.SetItem) -> None:
        characteristics = [e.name.upper() for e in item.expressions]
        for characteristic in characteristics:
            variable, value = TRANSACTION_CHARACTERISTICS[characteristic]
            self.variables.set(variable, value)

    def _show_variables(self, show: exp.Show) -> AllowedResult:
        rows = [(k, None if v is None else str(v)) for k, v in self.variables.list()]
        like = show.text("like")
        if like:
            rows = [(k, v) for k, v in rows if like_to_regex(like).match(k)]
        return rows, ["Variable_name", "Value"]

    def _show_status(self, show: exp.Show) -> AllowedResult:
        return [], ["Variable_name", "Value"]

    def _show_warnings(self, show: exp.Show) -> AllowedResult:
        return [], ["Level", "Code", "Message"]

    def _show_errors(self, show: exp.Show) -> AllowedResult:
        return [], ["Level", "Code", "Message"]
