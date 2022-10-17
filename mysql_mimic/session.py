from mysql_mimic.auth import User


class Session:
    """
    Abstract client session.

    This should be implemented by applications.
    """

    async def query(self, sql, attrs):  # pylint: disable=unused-argument
        """
        Process a SQL query.

        Args:
            sql (str): SQL statement from client
            attrs (dict): Arbitrary query attributes set by client
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

    async def close(self):
        """
        Close the session.

        This is called when the client closes the connection
        """

    async def init(self, connection):
        """
        Called when connection phase is complete.

        This is also called after a COM_CHANGE_USER command completes.

        Args:
            connection (mysql_mimic.connection.Connection): connection of the session
        """

    async def get_user(self, username):
        """
        Get a user by username.

        This is used during authentication.

        Returns:
            mysql_mimic.User: user
        """
        return User(name=username)

    async def set(self, **kwargs):
        """
        Set session variables.

        Args:
            **kwargs: mapping of variable names to values
        """

    async def show_columns(self, database, table):  # pylint: disable=unused-argument
        """
        Show column metadata.

        Args:
            database (str): database name
            table (str): table name
        Returns:
            list[mysql_mimic.results.Column|dict]: columns
        """
        return []

    async def rollback(self):
        """
        Roll back the current transaction, canceling its changes.
        """
