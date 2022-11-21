from contextvars import ContextVar

connection_id: ContextVar[int] = ContextVar("connection_id")
