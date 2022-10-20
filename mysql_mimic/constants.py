from mysql_mimic.types import Capabilities

DEFAULT_SERVER_CAPABILITIES = (
    Capabilities.CLIENT_PROTOCOL_41
    | Capabilities.CLIENT_DEPRECATE_EOF
    | Capabilities.CLIENT_CONNECT_WITH_DB
    | Capabilities.CLIENT_QUERY_ATTRIBUTES
    | Capabilities.CLIENT_CONNECT_ATTRS
    | Capabilities.CLIENT_PLUGIN_AUTH
    | Capabilities.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA
    | Capabilities.CLIENT_SECURE_CONNECTION
    | Capabilities.CLIENT_LONG_PASSWORD
)
