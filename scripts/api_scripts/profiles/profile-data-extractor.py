import json
import logging
import requests
import time
import pandas as pd
import sqlite3


# -------------------- external files url ---------------
from APIConnectors.Data365Connector import Data365Connector

user_profiles_db_url = "../../../api-data/user-profiles-full-test.db"
profiles_data_input_file_url = "../../../api-data/user_profiles.csv"
user_profiles_full_data_file_url = "../../../api-data/user-profiles-json.csv"
# ----------------------------------------------------
logging.basicConfig(level=logging.INFO)
connector = sqlite3.connect(user_profiles_db_url)
cursor = connector.cursor()


def get_users_profile_id_list():
    # profiles_df = pd.read_csv(profiles_data_input_file_url)
    ret_list = [55568794856]

    return ret_list


def init_user_profiles_table():
        """
        a function to start a new user's profiles table in the database
        """
        cursor.execute("""CREATE TABLE IF NOT EXISTS user_profiles (
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
        connector.commit()
        logging.info("user profiles table was created successfully")


def get_profile_data(profile_data):
    profile_id = profile_data.get("id", None)

    name = profile_data.get("full_name", None)
    if name is None:
        name = "unknown"

    nickname = profile_data.get("username", None)
    if nickname is None:
        nickname = "unknown"

    bio = profile_data.get("biography", None)
    if bio is None:
        bio = "No biography"

    post_count = profile_data.get("posts_count", -1)
    if post_count is None:
        post_count = -1

    followers_count = profile_data.get("followers_count", -1)
    if followers_count is None:
        followers_count = -1

    following_count = profile_data.get("followings_count", -1)
    if following_count is None:
        following_count = -1

    business = profile_data.get("is_business_account", "unknown")
    if business is None:
        business = "unknown"

    private = profile_data.get("is_private", "unknown")
    if private is None:
        private = "unknown"

    verified = profile_data.get("is_verified", "unknown")
    if verified is None:
        verified = "unknown"

    return profile_id, name, nickname, bio, post_count, followers_count, following_count, business, private, verified


def save_profile_data_to_database(profile_data):
    (profile_id, name, nickname, bio, post_count, followers_count, following_count, business, private, verified) = get_profile_data(profile_data)
    if profile_id is None:
        logging.warning(nickname + " ID is missing, could not insert into database")
    else:
        insert_query = '''
                    INSERT INTO user_profiles (ID, Name, Nickname, Bio, Post_Count, Follower_Count, Following_Count, Is_Business, Is_Private, Is_Verified)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    '''

        try:
            # Execute the SQL query with the provided parameters
            cursor.execute(insert_query, (str(profile_id), name, nickname, bio, post_count, followers_count, following_count, business, private, verified))
            connector.commit()
            logging.debug(nickname + " profile information was inserted to database successfully.")
            # saving json content to profiles data file (for future use maybe)
            with open(user_profiles_full_data_file_url, 'a') as file:
                profile_json_str = json.dumps(profile_data)
                file.write(profile_json_str + '\n')
                logging.debug(nickname + " profile information was inserted to profiles data file successfully.")
        except sqlite3.IntegrityError as e:
            logging.warning("insertion was failed, "+nickname + " profile id already exists in the database -Primary key.")
            connector.rollback()  # Rollback the transaction


def insert_profiles_data_to_database(profiles_list):
    api_connector = Data365Connector()
    profiles_count = 0

    logging.info("Getting profiles data process started...")
    for profile_id in profiles_list:
        logging.info("Getting data for instagram profile with id " + str(profile_id))
        profile_data = api_connector.get_profile_data_by_id(profile_id)

        if profile_data is not None:
            save_profile_data_to_database(profile_data)

        profiles_count = profiles_count + 1
        logging.info(
            "Getting profiles data in process.... " + str(profiles_count) + "/" + str(len(profiles_list)))
        # wait 1 sec (API can't receive more than 100 requests per sec)
        time.sleep(0.1)

    connector.close()
    logging.info(
        "Done getting profiles data, Database connection was closed successfully")


def start_program():
    init_user_profiles_table()
    profiles_id_list = get_users_profile_id_list()
    insert_profiles_data_to_database(profiles_id_list)


if __name__ == "__main__":
    start_program()
