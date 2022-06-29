from mysql_mimic.types import Capabilities

DEFAULT_SERVER_CAPABILITIES = (
    Capabilities.CLIENT_PROTOCOL_41
    | Capabilities.CLIENT_DEPRECATE_EOF
    | Capabilities.CLIENT_CONNECT_WITH_DB
)
