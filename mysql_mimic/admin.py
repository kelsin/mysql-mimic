import re
from typing import Optional, Any, Sequence, List

from mysql_mimic.session import Session
from mysql_mimic.charset import CharacterSet
from mysql_mimic.errors import MysqlError, ErrorCode
from mysql_mimic.results import ResultColumn, ResultSet, Column
from mysql_mimic.types import ColumnType
from mysql_mimic.variables import SystemVariables


class Admin:
    """
    Administration Statements (plus some other things)
    https://dev.mysql.com/doc/refman/8.0/en/sql-server-administration-statements.html
    """

    # Regex for finding "information functions" to replace in SQL statements
    REGEX_INFO_FUNC = re.compile(
        r"""
            (
              ((?P<func>
                (CONNECTION_ID)
                |(USER)
                |(CURRENT_USER)
                |(SYSTEM_USER)
                |(SESSION_USER)
                |(VERSION)
                |(DATABASE)
              )\(\))
            |
              (?P<current_user>CURRENT_USER)
            )
            (?=(?:[^"'`]*["'`][^"'`]*["'`])*[^"'`]*$)
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    # Regex for finding "session variables" to replace in SQL statements
    REGEX_SESSION_VAR = re.compile(
        r"""
            @@(?P<var>\w+)
            (?=(?:[^"'`]*["'`][^"'`]*["'`])*[^"'`]*$)
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    # Regex for determining if a statement is an "admin statement"
    REGEX_CMD = re.compile(
        r"^\s*(?P<cmd>(SET\s)|(SHOW\s)|(ROLLBACK))(?P<this>.*)$",
        re.IGNORECASE | re.VERBOSE,
    )

    # Regex for parsing a "SET VARIABLES" statement
    # Not yet supported:
    # 1. Setting variable to result of scalar subquery
    # 2. Setting variable to another variable
    # 3. Setting multiple variables at once
    REGEX_SET_VAR = re.compile(
        r"""
        ^\s*
        (
            (?P<global>(GLOBAL\s+)|(@@GLOBAL\.))
            |(?P<persist>(PERSIST\s+)|(@@PERSIST\.))
            |(?P<persist_only>(PERSIST_ONLY\s+)|(@@PERSIST_ONLY\.))
            |(?P<session>(SESSION\s+)|(@@SESSION\.)|(@@))
            |(?P<user>@)
        )?
        (?P<var_name>(\w+))
        \s*
        =
        \s*(?P<value>.*?)
        $
    """,
        re.IGNORECASE | re.VERBOSE,
    )

    # Regex for parsing a "SET NAMES" statement
    REGEX_SET_NAMES = re.compile(
        r"""
        ^\s*
        NAMES\s+
        '?(?P<charset_name>\w+)'?
        \s*
        (
            (COLLATE\s+'?(?P<collation_name>\w+)'?)
            |
            (DEFAULT)
        )?
        $
    """,
        re.IGNORECASE | re.VERBOSE,
    )

    # Regex for parsing a "SHOW VARIABLES" statement
    REGEX_SHOW_VARS = re.compile(
        r"""
        ^\s*
        (
            (?P<global>GLOBAL)
            |(?P<session>SESSION)
            \s*
        )?
        VARIABLES\s*
        (LIKE\s*'(?P<like>[\w%]+)'\s*)?
        $
    """,
        re.IGNORECASE | re.VERBOSE,
    )

    # Regex for parsing a "SHOW COLUMNS" statement
    REGEX_SHOW_COLUMNS = re.compile(
        r"""
        ^\s*
        ((?P<extended>EXTENDED)\s*)?
        ((?P<full>FULL)\s*)?
        ((COLUMNS)|(FIELDS))\s*
        ((FROM)|(IN))\s*(`?(?P<impl_db_name>\w+)`?\.)?`?(?P<tbl_name>\w+)`?\s*
        (((FROM)|(IN))\s*`?(?P<expl_db_name>\w+)`?\s*)?
        (LIKE\s*'(?P<like>[\w%]+)'\s*)?
        $
    """,
        re.IGNORECASE | re.VERBOSE,
    )

    # Regex for parsing a "SHOW INDEX" statement
    REGEX_SHOW_INDEX = re.compile(
        r"""
        ^\s*
        ((?P<extended>EXTENDED)\s*)?
        ((?P<full>FULL)\s*)?
        ((INDEX)|(INDEXES)|(KEYS))\s*
        ((FROM)|(IN))\s*(`?(?P<impl_db_name>\w+)`?\.)?`?(?P<tbl_name>\w+)`?\s*
        (((FROM)|(IN))\s*`?(?P<expl_db_name>\w+)`?\s*)?
        $
    """,
        re.IGNORECASE | re.VERBOSE,
    )

    # Regex for parsing a "SHOW TABLES" statement
    REGEX_SHOW_TABLES = re.compile(
        r"""
        ^\s*
        ((?P<extended>EXTENDED)\s*)?
        ((?P<full>FULL)\s*)?
        TABLES\s*
        (((FROM)|(IN))\s*`?(?P<db_name>\w+)`?\s*)?
        (LIKE\s*'(?P<like>[\w%]+)'\s*)?
        $
    """,
        re.IGNORECASE | re.VERBOSE,
    )

    # Regex for parsing a "SHOW DATABASE" statement
    REGEX_SHOW_DATABASES = re.compile(
        r"""
        ^\s*
        ((DATABASES)|(SCHEMAS))\s*
        (LIKE\s*'(?P<like>[\w%]+)'\s*)?
        $
    """,
        re.IGNORECASE | re.VERBOSE,
    )

    def __init__(
        self, connection_id: int, session: Session, variables: SystemVariables
    ):
        self.connection_id = connection_id
        self.session = session
        self.database: Optional[str] = None
        self.username: Optional[str] = None
        self.vars = variables

    def replace_variables(self, sql: str) -> str:
        sql = self.REGEX_INFO_FUNC.sub(self._replace_info_func, sql)
        return self.REGEX_SESSION_VAR.sub(self._replace_session_var, sql)

    def _replace_info_func(self, matchobj: re.Match) -> str:
        func = (matchobj.group("func") or matchobj.group("current_user")).lower()
        if func == "connection_id":
            return str(self.connection_id)
        if func in {"user", "session_user", "system_user"}:
            return f"'{self.vars.external_user}'" if self.vars.external_user else "NULL"
        if func in {"current_user"}:
            return f"'{self.username}'" if self.username else "NULL"
        if func == "version":
            return f"'{self.vars.mysql_version}'"
        if func == "database":
            return f"'{self.database}'" if self.database else "NULL"
        raise MysqlError(f"Failed to parse system information function: {func}")

    def _replace_session_var(self, matchobj: re.Match) -> str:
        var = matchobj.group("var").lower()
        if var in self.vars:
            return f"'{self.vars[var]}'"
        raise MysqlError(f"Unknown variable: {var}", ErrorCode.UNKNOWN_SYSTEM_VARIABLE)

    async def parse(self, sql: str) -> Optional[ResultSet]:
        m = self.REGEX_CMD.match(sql)
        if not m:
            return None

        cmd = m.group("cmd").strip().lower()
        this = m.group("this").strip().lower()

        if cmd == "set":
            return await self._parse_set(this)
        if cmd == "show":
            return await self._parse_show(this)
        if cmd == "rollback":
            await self.session.rollback()
            return ResultSet([], [])
        raise MysqlError("Failed to parse command", ErrorCode.PARSE_ERROR)

    async def _parse_set(self, this: str) -> Optional[ResultSet]:
        m = self.REGEX_SET_NAMES.match(this)
        if m:
            return await self._parse_set_names(
                charset_name=m.group("charset_name"),
                collation_name=m.group("collation_name"),
            )

        m = self.REGEX_SET_VAR.match(this)
        if m:
            return await self._parse_set_variables(
                global_=m.group("global"),
                persist=m.group("persist"),
                persist_only=m.group("persist_only"),
                session=m.group("session"),
                user=m.group("user"),
                var_name=m.group("var_name"),
                value=m.group("value"),
            )

        raise MysqlError("Unsupported SET command", ErrorCode.NOT_SUPPORTED_YET)

    async def _set_variable(self, key: str, val: Any) -> None:
        self.vars[key] = val
        await self.session.set(**{key: val})

    async def _parse_set_names(
        self, charset_name: str, collation_name: str
    ) -> Optional[ResultSet]:
        await self._set_variable("character_set_client", charset_name)
        await self._set_variable("character_set_connection", charset_name)
        await self._set_variable("character_set_results", charset_name)
        await self._set_variable(
            "collation_connection",
            (collation_name or CharacterSet[charset_name].default_collation.name),
        )
        return ResultSet([], [])

    async def _parse_set_variables(
        self,
        global_: Optional[str],
        persist: Optional[str],
        persist_only: Optional[str],
        session: Optional[str],
        user: Optional[str],
        var_name: Optional[str],
        value: Optional[str],
    ) -> Optional[ResultSet]:  # pylint: disable=unused-argument
        if global_ or persist or persist_only or user:
            raise MysqlError(
                "Only setting session variables is supported",
                ErrorCode.NOT_SUPPORTED_YET,
            )

        assert value is not None
        assert var_name is not None
        result: Any

        value_lower = value.lower()
        if value_lower in {"true", "on"}:
            result = True
        elif value_lower in {"false", "off"}:
            result = False
        elif value_lower in {"default", "null"}:
            result = self.vars.defaults.get(var_name)
        elif value[0] == value[-1] == "'":
            result = value[1:-1]
        else:
            try:
                result = int(value)
            except ValueError:
                try:
                    result = float(value)
                except ValueError as e:
                    raise MysqlError(
                        f"Unexpected variable value: {value}",
                        ErrorCode.WRONG_VALUE_FOR_VAR,
                    ) from e

        await self._set_variable(var_name, result)
        return ResultSet([], [])

    async def _parse_show(self, this: str) -> Optional[ResultSet]:
        m = self.REGEX_SHOW_VARS.match(this)
        if m:
            return await self._parse_show_variables(
                global_=bool(m.group("global")),
                session=bool(m.group("session")),
                like=self._like_to_regex(m.group("like")),
            )
        m = self.REGEX_SHOW_COLUMNS.match(this)
        if m:
            return await self._parse_show_columns(
                extended=bool(m.group("extended")),
                full=bool(m.group("full")),
                db_name=m.group("expl_db_name") or m.group("impl_db_name"),
                tbl_name=m.group("tbl_name"),
                like=self._like_to_regex(m.group("like")),
            )
        m = self.REGEX_SHOW_INDEX.match(this)
        if m:
            return await self._parse_show_index(
                extended=bool(m.group("extended")),
                db_name=m.group("expl_db_name") or m.group("impl_db_name"),
                tbl_name=m.group("tbl_name"),
            )
        m = self.REGEX_SHOW_TABLES.match(this)
        if m:
            return await self._parse_show_tables(
                extended=bool(m.group("extended")),
                full=bool(m.group("full")),
                db_name=m.group("db_name"),
                like=self._like_to_regex(m.group("like")),
            )
        m = self.REGEX_SHOW_DATABASES.match(this)
        if m:
            return await self._parse_show_databases(
                like=self._like_to_regex(m.group("like")),
            )

        raise MysqlError(
            f"Unsupported SHOW command: {this}", ErrorCode.NOT_SUPPORTED_YET
        )

    def _like_to_regex(self, like: Optional[str]) -> re.Pattern:
        if like is None:
            return re.compile(r".*")
        like = like.replace("%", ".*")
        like = like.replace("_", ".")
        return re.compile(like)

    async def _parse_show_columns(
        self,
        extended: bool,
        full: bool,
        db_name: Optional[str],
        tbl_name: Optional[str],
        like: re.Pattern,
    ) -> Optional[ResultSet]:  # pylint: disable=unused-argument
        db_name = db_name or self.database
        if not db_name:
            raise MysqlError("No database selected", ErrorCode.NO_DB_ERROR)

        columns = [
            c if isinstance(c, Column) else Column(**c)
            for c in await self.session.show_columns(db_name, tbl_name or "")
        ]
        columns = [c for c in columns if like.match(c.name)]

        result_columns = [
            ResultColumn("Field", ColumnType.STRING),
            ResultColumn("Type", ColumnType.STRING),
            ResultColumn("Null", ColumnType.STRING),
            ResultColumn("Key", ColumnType.STRING),
            ResultColumn("Default", ColumnType.STRING),
            ResultColumn("Extra", ColumnType.STRING),
        ]
        if full:
            result_columns.extend(
                [
                    ResultColumn("Collation", ColumnType.STRING),
                    ResultColumn("Privileges", ColumnType.STRING),
                    ResultColumn("Comment", ColumnType.STRING),
                ]
            )
        rows = []
        for c in columns:
            row = [c.name, c.type, c.null, c.key, c.default, c.extra]
            if full:
                row.extend(
                    [
                        c.collation,
                        c.privileges,
                        c.comment,
                    ]
                )
            rows.append(row)
        return ResultSet(rows=rows, columns=result_columns)

    async def _parse_show_index(
        self, extended: bool, db_name: Optional[str], tbl_name: Optional[str]
    ) -> ResultSet:  # pylint: disable=unused-argument
        result_columns = [
            ResultColumn("Table", ColumnType.STRING),
            ResultColumn("Non_unique", ColumnType.TINY),
            ResultColumn("Key_name", ColumnType.STRING),
            ResultColumn("Seq_in_index", ColumnType.LONGLONG),
            ResultColumn("Column_name", ColumnType.STRING),
            ResultColumn("Collation", ColumnType.STRING),
            ResultColumn("Cardinality", ColumnType.LONGLONG),
            ResultColumn("Sub_part", ColumnType.STRING),
            ResultColumn("Packed", ColumnType.STRING),
            ResultColumn("Null", ColumnType.STRING),
            ResultColumn("Index_type", ColumnType.STRING),
            ResultColumn("Comment", ColumnType.STRING),
            ResultColumn("Index_comment", ColumnType.STRING),
            ResultColumn("Visible", ColumnType.STRING),
            ResultColumn("Expression", ColumnType.STRING),
        ]
        return ResultSet(rows=[], columns=result_columns)

    async def _parse_show_tables(
        self, extended: bool, full: bool, db_name: Optional[str], like: re.Pattern
    ) -> ResultSet:  # pylint: disable=unused-argument
        db_name = db_name or self.database

        if not db_name:
            raise MysqlError("No database selected", ErrorCode.NO_DB_ERROR)

        rows: List[Sequence[str]] = [
            (t,) for t in await self.session.show_tables(db_name) if like.match(t)
        ]
        columns = [ResultColumn(f"Tables_in_{db_name}", ColumnType.STRING)]

        if full:
            rows = [(r[0], "BASE TABLE") for r in rows]
            columns.append(ResultColumn("Table_type", ColumnType.STRING))

        return ResultSet(rows=rows, columns=columns)

    async def _parse_show_databases(self, like: re.Pattern) -> ResultSet:
        databases = await self.session.show_databases()
        rows = [(db,) for db in databases if like.match(db)]
        return ResultSet(
            rows=rows, columns=[ResultColumn("Database", ColumnType.STRING)]
        )

    async def _parse_show_variables(
        self, global_: bool, session: bool, like: re.Pattern
    ) -> ResultSet:  # pylint: disable=unused-argument
        rows = list(self.vars.items())
        rows = [(k, v) for k, v in rows if like.match(k)]
        return ResultSet(
            rows=rows,
            columns=[
                ResultColumn("Variable_name", ColumnType.STRING),
                ResultColumn("Value", ColumnType.STRING),
            ],
        )
