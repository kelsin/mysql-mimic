from __future__ import annotations

import io
from copy import copy
from hashlib import sha1
import logging
import secrets
from dataclasses import dataclass
from typing import Optional, Dict, AsyncGenerator, Union, Tuple

from mysql_mimic.types import read_str_null
from mysql_mimic.utils import xor

logger = logging.getLogger(__name__)


@dataclass
class Forbidden:
    msg: Optional[str] = None


@dataclass
class Success:
    authenticated_as: str


@dataclass
class User:
    name: str
    auth_string: Optional[str] = None
    auth_plugin: Optional[str] = None


@dataclass
class AuthInfo:
    username: str
    data: bytes
    user: User
    connect_attrs: Dict[str, str]
    client_plugin_name: Optional[str]
    handshake_auth_data: Optional[bytes]
    handshake_plugin_name: str

    def copy(self, data: bytes) -> AuthInfo:
        new = copy(self)
        new.data = data
        return new


Decision = Union[Success, Forbidden, bytes]
AuthState = AsyncGenerator[Decision, AuthInfo]


class AuthPlugin:
    name = ""
    client_plugin_name: Optional[str] = None  # None means any

    async def auth(self, auth_info: Optional[AuthInfo] = None) -> AuthState:
        """
        Begin the authentication lifecycle.

        Returns:
            typing.AsyncGenerator[Success|Forbidden|bytes, AuthInfo]:
        """
        yield Forbidden()

    async def start(
        self, auth_info: Optional[AuthInfo] = None
    ) -> Tuple[Decision, AuthState]:
        state = self.auth(auth_info)
        data = await state.__anext__()
        return data, state


class GullibleAuthPlugin(AuthPlugin):
    name = "mysql_mimic_gullible"

    async def auth(self, auth_info: Optional[AuthInfo] = None) -> AuthState:
        if not auth_info:
            auth_info = yield b"\x00" * 20  # 20 bytes of filler to be ignored
        yield Success(authenticated_as=auth_info.username)


class AbstractMysqlClearPasswordAuthPlugin(AuthPlugin):
    name = "abstract_mysql_clear_password"
    client_plugin_name = "mysql_clear_password"

    async def auth(self, auth_info: Optional[AuthInfo] = None) -> AuthState:
        if not auth_info:
            auth_info = yield b"\x00" * 20  # 20 bytes of filler to be ignored

        r = io.BytesIO(auth_info.data)
        password = read_str_null(r).decode()
        authenticated_as = await self.check(auth_info.username, password)
        if authenticated_as is not None:
            yield Success(authenticated_as)
        else:
            yield Forbidden()

    async def check(self, username: str, password: str) -> Optional[str]:
        return username


class MysqlNativePasswordAuthPlugin(AuthPlugin):
    name = "mysql_native_password"
    client_plugin_name = "mysql_native_password"

    async def auth(self, auth_info: Optional[AuthInfo] = None) -> AuthState:
        if (
            auth_info
            and auth_info.handshake_plugin_name == self.name
            and auth_info.handshake_auth_data
        ):
            # mysql_native_password can reuse the nonce from the initial handshake
            nonce = auth_info.handshake_auth_data
        else:
            nonce = secrets.token_bytes(20)
            auth_info = yield nonce

        scramble = auth_info.data
        user = auth_info.user

        # Empty password quick path
        if not scramble:
            if not user.auth_string:
                yield Success(authenticated_as=user.name)
                return
            yield Forbidden()
            return

        if not user.auth_string:
            yield Forbidden()
            return

        # From docs,
        # response.data should be:
        #   SHA1(password) XOR SHA1("20-bytes random data from server" <concat> SHA1(SHA1(password)))
        # auth_string should be:
        #   SHA1(SHA1(password))
        sha1_sha1_password = bytes.fromhex(user.auth_string)
        sha1_sha1_with_nonce = sha1(nonce + sha1_sha1_password).digest()
        rcvd_sha1_password = xor(scramble, sha1_sha1_with_nonce)
        if sha1(rcvd_sha1_password).digest() == sha1_sha1_password:
            yield Success(user.name)
        else:
            yield Forbidden()


def get_mysql_native_password_auth_string(password: str) -> str:
    return sha1(sha1(password.encode("utf-8")).digest()).hexdigest()
