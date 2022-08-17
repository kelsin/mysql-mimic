import re

from mysql_mimic.charset import CharacterSet, Collation
from mysql_mimic.errors import MysqlError, ErrorCode
from mysql_mimic.results import ResultColumn, ResultSet, Column
from mysql_mimic.types import ColumnType


class Admin:
    """
    Administration Statements (plus some other things)
    https://dev.mysql.com/doc/refman/8.0/en/sql-server-administration-statements.html

    Args:
        connection_id (int): connection ID
        session (mysql_mimic.session.Session): session
        variable_defaults (dict): default values for session variables
    """

    # Regex for finding "information functions" to replace in SQL statements
    REGEX_INFO_FUNC = re.compile(
        r"""
            (?P<func>
                (CONNECTION_ID)
                |(USER)
                |(SYSTEM_USER)
                |(SESSION_USER)
                |(VERSION)
            )\(\)
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
        r"^\s*(?P<cmd>(SET)|(SHOW))\s(?P<this>.*)$", re.IGNORECASE | re.VERBOSE
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

    def __init__(self, connection_id, session, variable_defaults=None):
        self.connection_id = connection_id
        self.session = session
        self.database = None
        self.username = None
        self.variable_defaults = variable_defaults or {
            "version": "8.0.29",
            "version_comment": "mysql-mimic",
            "character_set_client": CharacterSet.utf8mb4.name,
            "character_set_results": CharacterSet.utf8mb4.name,
            "character_set_server": CharacterSet.utf8mb4.name,
            "collation_server": Collation.utf8mb4_general_ci.name,
            "collation_database": Collation.utf8mb4_general_ci.name,
        }
        self.variables = dict(**self.variable_defaults)

    @property
    def server_character_set(self):
        return CharacterSet[self.variables.get("character_set_results", "utf8mb4")]

    @server_character_set.setter
    def server_character_set(self, val):
        self.variables["character_set_results"] = val.name

    @property
    def client_character_set(self):
        return CharacterSet[self.variables.get("character_set_client", "utf8mb4")]

    @client_character_set.setter
    def client_character_set(self, val):
        self.variables["character_set_client"] = val.name

    @property
    def mysql_version(self):
        return self.variables["version"]

    def replace_variables(self, sql):
        sql = self.REGEX_INFO_FUNC.sub(self._replace_info_func, sql)
        return self.REGEX_SESSION_VAR.sub(self._replace_session_var, sql)

    def _replace_info_func(self, matchobj):
        func = matchobj.group("func").lower()
        if func == "connection_id":
            return str(self.connection_id)
        if func in {"user", "session_user", "system_user"}:
            return f"'{self.username}'"
        if func == "version":
            return f"'{self.mysql_version}'"
        raise MysqlError(f"Failed to parse system information function: {func}")

    def _replace_session_var(self, matchobj):
        var = matchobj.group("var").lower()
        if var in self.variables:
            return f"'{self.variables[var]}'"
        raise MysqlError(f"Unknown variable: {var}", ErrorCode.UNKNOWN_SYSTEM_VARIABLE)

    async def parse(self, sql):
        m = self.REGEX_CMD.match(sql)
        if not m:
            return None

        cmd = m.group("cmd").lower()
        this = m.group("this").lower()

        if cmd == "set":
            return await self._parse_set(this)
        if cmd == "show":
            return await self._parse_show(this)
        raise MysqlError("Failed to parse command", ErrorCode.PARSE_ERROR)

    async def _parse_set(self, this):
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

    async def _set_variable(self, key, val):
        self.variables[key] = val
        await self.session.set(**{key: val})

    async def _parse_set_names(self, charset_name, collation_name):
        await self._set_variable("character_set_client", charset_name)
        await self._set_variable("character_set_connection", charset_name)
        await self._set_variable("character_set_results", charset_name)
        await self._set_variable(
            "collation_connection",
            (collation_name or CharacterSet[charset_name].default_collation.name),
        )
        return ResultSet([], [])

    async def _parse_set_variables(
        self, global_, persist, persist_only, session, user, var_name, value
    ):  # pylint: disable=unused-argument
        if global_ or persist or persist_only or user:
            raise MysqlError(
                "Only setting session variables is supported",
                ErrorCode.NOT_SUPPORTED_YET,
            )

        value_lower = value.lower()
        if value_lower in {"true", "on"}:
            result = True
        elif value_lower in {"false", "off"}:
            result = False
        elif value_lower in {"default", "null"}:
            result = self.variable_defaults.get(var_name)
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

    async def _parse_show(self, this):
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

        raise MysqlError("Unsupported SHOW command", ErrorCode.NOT_SUPPORTED_YET)

    def _like_to_regex(self, like):
        if like is None:
            return re.compile(r".*")
        like = like.replace("%", ".*")
        like = like.replace("_", ".")
        return re.compile(like)

    async def _parse_show_columns(
        self, extended, full, db_name, tbl_name, like
    ):  # pylint: disable=unused-argument
        db_name = db_name or self.database
        if not db_name:
            raise MysqlError("No database selected", ErrorCode.NO_DB_ERROR)

        columns = await self.session.show_columns(db_name, tbl_name)
        columns = [c if isinstance(c, Column) else Column(**c) for c in columns]
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
        self, extended, db_name, tbl_name
    ):  # pylint: disable=unused-argument
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

    async def _parse_show_variables(
        self, global_, session, like
    ):  # pylint: disable=unused-argument
        rows = list(self.variables.items())
        rows = [(k, v) for k, v in rows if like.match(k)]
        return ResultSet(
            rows=rows,
            columns=[
                ResultColumn("Variable_name", ColumnType.STRING),
                ResultColumn("Value", ColumnType.STRING),
            ],
        )
