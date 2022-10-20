from typing import Dict, TYPE_CHECKING, Any, Sequence, Union

if TYPE_CHECKING:
    from mysql_mimic.results import AllowedResult, Column
    from mysql_mimic.connection import Connection


class Session:
    """
    Abstract client session.

    This should be implemented by applications.
    """

    async def query(
        self, sql: str, attrs: Dict[str, str]
    ) -> "AllowedResult":  # pylint: disable=unused-argument
        """
        Process a SQL query.

        Args:
            sql: SQL statement from client
            attrs: Arbitrary query attributes set by client
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

    async def close(self) -> None:
        """
        Close the session.

        This is called when the client closes the connection
        """

    async def init(self, connection: "Connection") -> None:
        """
        Called when connection phase is complete.

        This is also called after a COM_CHANGE_USER command completes.

        Args:
            connection: connection of the session
        """

    async def set(self, **kwargs: Dict[str, Any]) -> None:
        """
        Set session variables.

        Args:
            **kwargs: mapping of variable names to values
        """

    async def show_columns(
        self, database: str, table: str
    ) -> Sequence[Union["Column", dict]]:  # pylint: disable=unused-argument
        """
        Show column metadata.

        Args:
            database: database name
            table: table name
        Returns:
            columns
        """
        return []

    async def show_tables(
        self, database: str
    ) -> Sequence[str]:  # pylint: disable=unused-argument
        """
        Show table metadata.

        Args:
            database: database name
        Returns:
            table names
        """
        return []

    async def show_databases(self) -> Sequence[str]:
        """
        Show database metadata.

        Returns:
            database names
        """
        return []

    async def rollback(self) -> None:
        """
        Roll back the current transaction, canceling its changes.
        """
