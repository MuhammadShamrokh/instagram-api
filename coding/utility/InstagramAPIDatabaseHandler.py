from coding.loggers.Logger import logger
import sqlite3


class InstagramAPIDatabaseHandler:
    def __init__(self, url):
        self.database_url = url
        self.connector = sqlite3.connect(self.database_url)
        self.cursor = self.connector.cursor()

    def __del__(self):
        self._close_connection()

    def save_profile_to_database(self, table_name, profile_data_tuple):
        if len(profile_data_tuple) == 0:
            return

        # checking if table exist in database
        self.cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        # if table doesn't exist, we add the table to database
        if self.cursor.fetchone()[0] == 0:
            self._create_new_profile_table_in_db(table_name)

        # add the profile to the database
        self._insert_into_profile_table(table_name, profile_data_tuple)

    def save_profile_with_engagement_to_database(self, table_name, profile_tuple):
        if len(profile_tuple) == 0:
            return

        # checking if table exist in database
        self.cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        # if table doesn't exist, we add the table to database
        if self.cursor.fetchone()[0] == 0:
            self._create_new_profile_engagement_table_in_db(table_name)

        # add the profile to the database
        self._insert_into_profile_with_engagement_table(table_name, profile_tuple)

    def save_post_to_database(self, table_name, post_data_tuple, by_hashtag=False):
        if len(post_data_tuple) == 0:
            return

        # checking if table exist in database
        self.cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        # if table doesn't exist, we add the table to database

        # Post found using search by hashtag (we save the hashtag it was found from).
        if by_hashtag:
            if self.cursor.fetchone()[0] == 0:
                self._create_new_post_by_hashtag_table_in_db(table_name)

            self._insert_post_by_hashtag_into_table(table_name, post_data_tuple)
        else:
            # post was found using profile or post id. (not by hash)
            if self.cursor.fetchone()[0] == 0:
                self._create_new_post_table_in_db(table_name)

            self._insert_post_into_table(table_name, post_data_tuple)

    def delete_profile_from_database(self, table_name, profile_id):
        delete_query = f"""DELETE FROM {table_name} WHERE ID=?"""
        self.cursor.execute(delete_query, (profile_id,))
        self.connector.commit()
        logger.debug(
            "DatabaseHandler: profile " + str(profile_id) + " was deleted from database successfully.")

    def delete_database_table(self, table_name):
        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"
        self.cursor.execute(drop_table_query)
        self.connector.commit()

    def _insert_into_profile_table(self, table_name, profile_data_tuple):
        insert_query = f"""INSERT INTO {table_name} (ID, Name, Nickname, Bio, Post_Count, Follower_Count, Following_Count, Is_Business, Is_Private, Is_Verified)
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        try:
            # Execute the SQL query with the provided parameters
            self.cursor.execute(insert_query, profile_data_tuple)
            self.connector.commit()
            logger.debug("DatabaseHandler: "+profile_data_tuple[2] + "profile data was inserted to database successfully.")

        # profile id already exists error
        except sqlite3.IntegrityError as e:
            logger.warning(
                "DatabaseHandler: insertion was failed, " + profile_data_tuple[2] + " profile id already exists in the database -Primary key.")
            self.connector.rollback()  # Rollback the transaction

    def _insert_into_profile_with_engagement_table(self, table_name, profile_tuple):
        insert_query = f"""INSERT INTO {table_name} (ID, Name, Nickname, Bio, Post_Count, Follower_Count, Following_Count, Is_Business, Is_Private, Is_Verified, Posts_Last_Month, Engagement_Last_Month)
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        try:
            # Execute the SQL query with the provided parameters
            self.cursor.execute(insert_query, profile_tuple)
            self.connector.commit()
            logger.debug("DatabaseHandler: "+profile_tuple[2] + "profile data was inserted to database successfully.")

        # profile id already exists error
        except sqlite3.IntegrityError as e:
            logger.warning(
                "DatabaseHandler: insertion was failed, " + profile_tuple[1] + " profile id already exists in the database -Primary key.")
            self.connector.rollback()  # Rollback the transaction

    def _insert_post_into_table(self, table_name, post_data_tuple):
        insert_query = f"""INSERT INTO {table_name} (ID, Caption, Owner_ID, Owner_Username, Likes_Count, Comments_Count, Has_Video, Publication_Date, Publication_Timestamp, Location_ID)
                                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        try:
            # Execute the SQL query with the provided parameters
            self.cursor.execute(insert_query, post_data_tuple)
            self.connector.commit()
            logger.debug("DatabaseHandler: Post with id " + post_data_tuple[0] + " was inserted to database successfully.")

        # profile id already exists error
        except sqlite3.IntegrityError as e:
            logger.warning(
                "DatabaseHandler: insertion was failed, " + post_data_tuple[0] + " post id already exists in the database -Primary key.")
            self.connector.rollback()  # Rollback the transaction

    def _insert_post_by_hashtag_into_table(self, table_name, post_data_tuple):
        insert_query = f"""INSERT INTO {table_name} (ID, Caption, Owner_ID, Owner_Username, Likes_Count, Comments_Count, Has_Video, Publication_Date, Publication_Timestamp, Location_ID, Hashtag)
                                              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        try:
            # Execute the SQL query with the provided parameters
            self.cursor.execute(insert_query, post_data_tuple)
            self.connector.commit()
            logger.debug(
                "DatabaseHandler: Post by hashtag with id " + post_data_tuple[0] + " was inserted to database successfully.")

        # profile id already exists error
        except sqlite3.IntegrityError as e:
            logger.warning(
                "DatabaseHandler: insertion was failed, " + post_data_tuple[
                    0] + " post id already exists in the database -Primary key.")
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
        logger.info(f"DatabaseHandler: {table_name} table was created successfully")

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
                                                    Posts_Amount integer,
                                                    Engagement text)""")
        self.connector.commit()
        logger.info(f"DatabaseHandler: {table_name} table was created successfully")

    def _create_new_post_table_in_db(self, table_name):
        self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                                                            ID text PRIMARY KEY,
                                                            Caption text,
                                                            Owner_ID text,
                                                            Owner_Username text,
                                                            Likes_Count integer,
                                                            Comments_Count integer,
                                                            Has_Video integer,
                                                            publication_Date text,
                                                            Publication_Timestamp text,
                                                            Location_ID text)""")
        self.connector.commit()
        logger.info(f"DatabaseHandler: {table_name} table was created successfully")

    def _create_new_post_by_hashtag_table_in_db(self, table_name):
        self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} (
                                                            ID text PRIMARY KEY,
                                                            Caption text,
                                                            Owner_ID text,
                                                            Owner_Username text,
                                                            Likes_Count integer,
                                                            Comments_Count integer,
                                                            Has_Video integer,
                                                            publication_Date text,
                                                            Publication_Timestamp text,
                                                            Location_ID text,
                                                            Hashtag text)""")
        self.connector.commit()
        logger.info(f"DatabaseHandler: {table_name} table was created successfully")

    def _close_connection(self):
        self.connector.close()


