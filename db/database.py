import sys
import time

import mysql.connector
from loguru import logger

create_table_methods = []


def register_create_table_method(func):
    """
    A decorator function that registers a function to create a table in the database.

    Parameters
    ----------
    func : function
        the function to register
    """
    create_table_methods.append(func)
    return func


class Database:
    """
    A class used to represent a connection to a MySQL database.

    Attributes
    ----------
    host : str
        the hostname of the MySQL server
    user : str
        the username to connect to the MySQL server
    password : str
        the password to connect to the MySQL server
    database : str
        the name of the database to connect to
    connection : mysql.connector.connection.MySQLConnection or None
        the connection object to the MySQL server
    """

    def __init__(self, host, user, password, database):
        """
        Constructs all the necessary attributes for the Database object.

        Parameters
        ----------
        host : str
            the hostname of the MySQL server
        user : str
            the username to connect to the MySQL server
        password : str
            the password to connect to the MySQL server
        database : str
            the name of the database to connect to
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self._last_used = 0  # Timestamp of last successful query
        self._max_idle_seconds = 60  # Reconnect if idle longer than this

    def connect(self):
        """
        Establishes a connection to the MySQL server.
        """
        if self.connection is not None:
            return
        else:
            try:
                self.connection = mysql.connector.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    connection_timeout=10,  # Fail fast on connection issues
                    autocommit=True,  # Don't hold transactions open
                )
                logger.info("Connected to MySQL server")
            except mysql.connector.Error as error:
                logger.error(f"There was an error connecting to MySQL server: {error}")
                sys.exit()

    def ensure_connection(self) -> None:
        """Ensure connection is alive, reconnect if stale.

        Call this before database operations in long-running loops where the
        connection may have timed out (e.g., during API calls or NAT timeout).

        Reconnects if the connection has been idle for more than _max_idle_seconds.
        This avoids NAT/firewall timeouts without reconnecting on every query.
        """
        now = time.time()
        idle_time = now - self._last_used

        # If connection is old or doesn't exist, reconnect
        if self.connection is None or idle_time > self._max_idle_seconds:
            if self.connection is not None:
                try:
                    self.connection.close()
                    logger.debug(f"Closed stale connection (idle {idle_time:.0f}s)")
                except Exception:
                    pass  # Ignore errors closing dead connection
                self.connection = None
            self.connect()

        self._last_used = now

    def close(self):
        """
        Closes the connection to the MySQL server.
        """
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Connection closed")

    def drop_table(self, table_name):
        """
        Drops a table from the database if it exists.

        Parameters
        ----------
        table_name : str
            the name of the table to drop
        """
        cursor = self.connection.cursor()
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        self.connection.commit()
        cursor.close()
        logger.info(f"Table {table_name} dropped")

    def create_table(self, query):
        """
        Creates a table in the database using the provided SQL query.

        Parameters
        ----------
        query : str
            the SQL query to create the table
        """
        cursor = self.connection.cursor()
        cursor.execute(query)
        self.connection.commit()
        cursor.close()
        logger.info("Table created")

    def execute_query(self, query, params=None):
        """
        Executes a SQL query on the database.

        Parameters
        ----------
        query : str
            the SQL query to execute
        params : tuple, optional
            the parameters to use with the SQL query
        """
        self.ensure_connection()
        try:
            cursor = self.connection.cursor()
            logger.debug("Executing query on MySQL server")
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
            cursor.close()
        except mysql.connector.Error as error:
            logger.error(f"Error executing query: {error}")
            # sys.exit()

    def execute_select_query(self, query, params=None):
        """
        Executes a SELECT SQL query on the database and returns the results.

        Parameters
        ----------
        query : str
            the SQL query to execute
        params : tuple, optional
            the parameters to use with the SQL query

        Returns
        -------
        list
            the results of the query
        """
        self.ensure_connection()
        result = []
        try:
            cursor = self.connection.cursor()
            logger.debug("Connected to MySQL server")
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchall()
        except mysql.connector.Error as error:
            logger.error(f"There was an error executing the query: {error}")
            self.connection.rollback()
        return result

    def create_all_tables(self):
        """
        Creates all tables in the database.
        """
        for method in create_table_methods:
            method(self)

    @register_create_table_method
    def create_artists_table(self, table_name="artists"):
        """
        Creates the artists table in the database.

        Parameters
        ----------
        table_name : str, optional
            the name of the table to create (default is "artists")
        """
        self.execute_query("SET FOREIGN_KEY_CHECKS = 0")
        self.drop_table(table_name)
        artists_ddl = """CREATE TABLE IF NOT EXISTS artists(
        id INTEGER PRIMARY KEY AUTO_INCREMENT
        , artist VARCHAR(255) NOT NULL
        , last_fm_id VARCHAR(255)
        , discogs_id VARCHAR(255)
        , musicbrainz_id VARCHAR(255)
        )"""
        self.create_table(artists_ddl)
        self.execute_query("SET FOREIGN_KEY_CHECKS = 1")

    @register_create_table_method
    def create_track_data_table(self, table_name="track_data"):
        """
        Creates the track_data table in the database.

        Parameters
        ----------
        plex_server : str
            the name of the Plex server
        table_name : str, optional
            the name of the table to create (default is "track_data")
        """
        self.execute_query("SET FOREIGN_KEY_CHECKS = 0")
        self.drop_table("track_data")
        track_data_ddl = """
        CREATE TABLE IF NOT EXISTS track_data(
        id INTEGER PRIMARY KEY AUTO_INCREMENT
        , title VARCHAR (1000) NOT NULL
        , artist VARCHAR (1000) NOT NULL
        , album VARCHAR (1000) NOT NULL
        , added_date VARCHAR (50)
        , filepath VARCHAR (500)
        , location VARCHAR (500)
        , bpm INTEGER
        , genre VARCHAR (1000)
        , artist_id INTEGER
        , plex_id INTEGER
        , musicbrainz_id VARCHAR(255)
        , acoustid VARCHAR(255)
        , FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE)"""
        self.create_table(track_data_ddl)
        ix_loc = """CREATE INDEX ix_loc ON track_data (location)"""
        ix_filepath = """CREATE INDEX ix_filepath ON track_data (filepath)"""
        ix_bpm = """CREATE INDEX ix_bpm ON track_data (bpm)"""
        ix_mbid = """CREATE INDEX ix_musicbrainz_id ON track_data (musicbrainz_id)"""
        ix_plex = """CREATE INDEX ix_plex_id ON track_data (plex_id)"""
        self.execute_query(ix_loc)
        self.execute_query(ix_filepath)
        self.execute_query(ix_bpm)
        self.execute_query(ix_mbid)
        self.execute_query(ix_plex)
        self.execute_query("SET FOREIGN_KEY_CHECKS = 1")

    @register_create_table_method
    def create_history_table(self, table_name="history"):
        """
        Creates the history table in the database.

        Parameters
        ----------
        table_name : str, optional
            the name of the table to create (default is "history")
        """
        self.execute_query("SET FOREIGN_KEY_CHECKS = 0")
        self.drop_table("history")
        history_ddl = """
        CREATE TABLE IF NOT EXISTS history(
        id INTEGER PRIMARY KEY AUTO_INCREMENT
        , tx_date DATE
        , records INTEGER (6)
        , latest_entry DATE)"""
        self.create_table(history_ddl)
        self.execute_query("SET FOREIGN_KEY_CHECKS = 1")

    # @register_create_table_method
    # def create_tags_table(self):
    #     """
    #     Creates the tags table in the database.
    #     """
    #     self.execute_query("SET FOREIGN_KEY_CHECKS = 0")
    #     self.drop_table("tags")
    #     tags_ddl = '''
    #     CREATE TABLE IF NOT EXISTS tags(
    #     id INTEGER PRIMARY KEY AUTO_INCREMENT
    #     , tag INTEGER (6)
    #     , artist_id INTEGER
    #
    #     , FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE
    #     , FOREIGN KEY (tag) REFERENCES genres(id) ON DELETE CASCADE)'''
    #     self.create_table(tags_ddl)
    #     self.execute_query("SET FOREIGN_KEY_CHECKS = 1")

    @register_create_table_method
    def create_similar_artists_table(self):
        """
        Creates the similar_artists table in the database.
        """
        self.execute_query("SET FOREIGN_KEY_CHECKS = 0")
        self.drop_table("similar_artists")
        similar_artists_ddl = """
        CREATE TABLE IF NOT EXISTS similar_artists(
        id INTEGER PRIMARY KEY AUTO_INCREMENT
        , artist_id INTEGER
        , similar_artist_id INTEGER
        , FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE
        , FOREIGN KEY (similar_artist_id) REFERENCES artists(id) ON DELETE CASCADE)"""
        self.create_table(similar_artists_ddl)
        self.execute_query("SET FOREIGN_KEY_CHECKS = 1")

    @register_create_table_method
    def create_genres_table(self):
        """
        Creates the genres table in the database.
        """
        self.execute_query("SET FOREIGN_KEY_CHECKS = 0")
        self.drop_table("genres")
        genres_ddl = """
        CREATE TABLE IF NOT EXISTS genres(
        id INTEGER PRIMARY KEY AUTO_INCREMENT
        , genre VARCHAR(1000) NOT NULL
        )
        """
        self.create_table(genres_ddl)
        self.execute_query("SET FOREIGN_KEY_CHECKS = 1")

    @register_create_table_method
    def create_track_genres_table(self):
        """
        Creates the track_genres table in the database.
        """
        self.execute_query("SET FOREIGN_KEY_CHECKS = 0")
        self.drop_table("track_genres")
        track_genres_ddl = """
        CREATE TABLE IF NOT EXISTS track_genres(
        id INTEGER PRIMARY KEY AUTO_INCREMENT
        , track_id INTEGER
        , genre_id INTEGER
        , FOREIGN KEY (track_id) REFERENCES track_data(id) ON DELETE CASCADE
        , FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
        )
        """
        self.create_table(track_genres_ddl)
        self.execute_query("SET FOREIGN_KEY_CHECKS = 1")

    @register_create_table_method
    def create_artist_genres_table(self):
        """
        Creates the artist_genres table in the database.
        """
        self.execute_query("SET FOREIGN_KEY_CHECKS = 0")
        self.drop_table("artist_genres")
        artist_genres_ddl = """
        CREATE TABLE IF NOT EXISTS artist_genres(
        id INTEGER PRIMARY KEY AUTO_INCREMENT
        , artist_id INTEGER
        , genre_id INTEGER
        , FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE
        , FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
        )
        """
        self.create_table(artist_genres_ddl)
        self.execute_query("SET FOREIGN_KEY_CHECKS = 1")

    def drop_all_tables(self):
        """
        Drops all tables in the database.
        """
        self.connect()
        self.execute_query("SET FOREIGN_KEY_CHECKS = 0")
        for method in create_table_methods:
            table_name = method.__name__.replace("create_", "").replace("_table", "")
            self.drop_table(table_name)
        self.execute_query("SET FOREIGN_KEY_CHECKS = 1")
        self.close()
