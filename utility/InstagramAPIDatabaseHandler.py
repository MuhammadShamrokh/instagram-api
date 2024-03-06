from APIConnectors.Data365Connector import Data365Connector
from loggers.Logger import logger
import sqlite3


class InstagramAPIDatabaseHandler:
    def __init__(self, url):
        self.database_url = url
        self.connector = sqlite3.connect(self.database_url)
        self.cursor = self.connector.cursor()
        self.api_connector = Data365Connector()

    def __del__(self):
        self._close_connection()

    def save_profile_to_database(self, table_name, profile_json):
        # checking if table exist in database
        self.cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        # if table doesn't exist, we add the table to database
        if self.cursor.fetchone()[0] == 0:
            self._create_new_profile_table_in_db(table_name)

        # add the profile to the database
        self._insert_into_profile_table(table_name, profile_json)

    def save_profile_with_month_engagement_to_database(self, table_name, profile_tuple):
        # checking if table exist in database
        self.cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        # if table doesn't exist, we add the table to database
        if self.cursor.fetchone()[0] == 0:
            self._create_new_profile_engagement_table_in_db(table_name)

        # add the profile to the database
        self._insert_into_profile_with_engagement_table(table_name, profile_tuple)

    def _insert_into_profile_table(self, table_name, profile_json):
        insert_query = f"""INSERT INTO {table_name} (ID, Name, Nickname, Bio, Post_Count, Follower_Count, Following_Count, Is_Business, Is_Private, Is_Verified)
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        try:
            # Execute the SQL query with the provided parameters
            self.cursor.execute(insert_query, self.api_connector.get_profile_data_tuple_from_json(profile_json))
            self.connector.commit()
            logger.info("DatabaseHandler: "+profile_json.get("username") + "profile data was inserted to database successfully.")

        # profile id already exists error
        except sqlite3.IntegrityError as e:
            logger.warning(
                "DatabaseHandler: insertion was failed, " + profile_json["username"] + " profile id already exists in the database -Primary key.")
            self.connector.rollback()  # Rollback the transaction

    def _insert_into_profile_with_engagement_table(self, table_name, profile_tuple):
        insert_query = f"""INSERT INTO {table_name} (ID, Name, Nickname, Bio, Post_Count, Follower_Count, Following_Count, Is_Business, Is_Private, Is_Verified, Posts_Last_Month, Engagement_Last_Month)
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        try:
            # Execute the SQL query with the provided parameters
            self.cursor.execute(insert_query, profile_tuple)
            self.connector.commit()
            logger.info("DatabaseHandler: "+profile_tuple[2] + "profile data was inserted to database successfully.")

        # profile id already exists error
        except sqlite3.IntegrityError as e:
            logger.warning(
                "DatabaseHandler: insertion was failed, " + profile_tuple[1] + " profile id already exists in the database -Primary key.")
            self.connector.rollback()  # Rollback the transaction

    def _create_new_profile_table_in_db(self, table_name):
        self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                                            ID text PRIMARY KEY,
                                            Name text,
                                            Nickname text,
                                            Bio text,
                                            Post_Count integer,
                                            Follower_Count integer,
                                            Following_Count integer,
                                            Is_Business text,
                                            Is_Private text,
                                            Is_Verified text)""")
        self.connector.commit()
        logger.info("DatabaseHandler: user profiles table was created successfully")

    def _create_new_profile_engagement_table_in_db(self, table_name):
        self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                                                    ID text PRIMARY KEY,
                                                    Name text,
                                                    Nickname text,
                                                    Bio text,
                                                    Post_Count integer,
                                                    Follower_Count integer,
                                                    Following_Count integer,
                                                    Is_Business text,
                                                    Is_Private text,
                                                    Is_Verified text,
                                                    Posts_Last_month integer,
                                                    Engagement_Last_Month)""")
        self.connector.commit()
        logger.info("DatabaseHandler: user profiles with engagement table was created successfully")

    def _close_connection(self):
        self.connector.close()


