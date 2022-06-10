from mysql_mimic import types

DEFAULT_SERVER_CAPABILITIES = (
    types.Capabilities.CLIENT_PROTOCOL_41 | types.Capabilities.CLIENT_DEPRECATE_EOF
)
