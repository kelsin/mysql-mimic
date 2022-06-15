class Session:
    """
    Abstract client session.

    This should be implemented by applications.
    """

    async def query(self, sql):  # pylint: disable=unused-argument
        """
        Process a SQL query.

        Args:
            sql (str): SQL statement from client
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
        Initialize the session.

        Args:
            connection (Connection): connection of the session
        """
