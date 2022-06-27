import re

from mysql_mimic.charset import CharacterSet
from mysql_mimic.errors import MysqlError, ErrorCode

REGEX_SET = re.compile(r"^\s*SET\s(?P<cmd>.*)$", re.IGNORECASE)

# Not yet supported:
# 1. Setting variable to result of scalar subquery
# 2. Setting variable to another variable
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
REGEX_SET_CHARACTER_SET = re.compile(r"^$", re.IGNORECASE)


def parse_set_command(sql, defaults=None):
    m = REGEX_SET.match(sql)
    if not m:
        return None

    defaults = defaults or {}
    result = {}

    cmd = m.group("cmd")
    m = REGEX_SET_NAMES.match(cmd)
    if m:
        charset_name = m.group("charset_name")
        collation_name = m.group("collation_name")
        result["character_set_client"] = charset_name
        result["character_set_connection"] = charset_name
        result["character_set_results"] = charset_name
        result["collation_connection"] = (
            collation_name or CharacterSet[charset_name].default_collation.name
        )
        return result

    cmds = cmd.split(",")
    matched = False
    for cmd in cmds:
        m = REGEX_SET_VAR.match(cmd)
        if m:
            matched = True
            for forbidden in ["global", "persist", "persist_only", "user"]:
                if m.group(forbidden):
                    raise MysqlError(
                        f"Setting {forbidden} variables not supported",
                        ErrorCode.NOT_SUPPORTED_YET,
                    )

            var_name = m.group("var_name")

            value = m.group("value")
            value_lower = value.lower()
            if value_lower in {"true", "on"}:
                result[var_name] = True
            elif value_lower in {"false", "off"}:
                result[var_name] = False
            elif value_lower == "null":
                result[var_name] = None
            elif value_lower == "default":
                result[var_name] = defaults.get(var_name)
            elif value[0] == value[-1] == "'":
                result[var_name] = value[1:-1]
            else:
                try:
                    result[var_name] = int(value)
                    continue
                except ValueError:
                    pass
                try:
                    result[var_name] = float(value)
                except ValueError as e:
                    raise MysqlError(
                        f"Unexpected variable value: {value}",
                        ErrorCode.WRONG_VALUE_FOR_VAR,
                    ) from e

    if not matched:
        raise MysqlError("Failed to parse SET command", ErrorCode.PARSE_ERROR)

    return result
