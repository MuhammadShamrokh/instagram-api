from loggers.Logger import logger
from APIConnectors.Data365Connector import Data365Connector
from utility.InstagramAPIDatabaseHandler import InstagramAPIDatabaseHandler
import pandas as pd
import sqlite3
from datetime import datetime, timedelta

api_connector = Data365Connector()
database_connector = InstagramAPIDatabaseHandler('../../../api-data/profiles/profiles.db')
MAX_AMOUNT = 100
THRESHOLD = 7183
# calculating one week ago date
FROM_DATE = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')


def save_profile_with_engagement_in_db(profile_data, post_amount, engagement_count):
    profile_data_tuple = (profile_data['ID'], profile_data['Name'], profile_data['Nickname'], profile_data['Bio'],
                          profile_data['Post_Count'], profile_data['Follower_Count'], profile_data['Following_Count'],
                          profile_data['Is_Business'], profile_data['Is_Private'], profile_data['Is_Verified'],
                          post_amount, engagement_count)

    database_connector.save_profile_with_month_engagement_to_database("NFL_profiles_with_month_engagement",
                                                                      profile_data_tuple)


def calculate_profiles_engagement_store_in_db(profiles_df):
    for idx, profile in profiles_df.iterrows():
        profile_id = profile['ID']
        profile_engagement_during_last_month = 0

        logger.info("calculating profile with " + str(profile_id) + " id engagement. (" + str(idx) + "/" + str(
            len(profiles_df)+THRESHOLD) + ")")
        # fetching all profile posts from last month
        profile_posts_lst = api_connector.get_profile_posts(profile_id, MAX_AMOUNT, FROM_DATE)

        # scanning all posts to discover who interacted with same profile id
        for post in profile_posts_lst:
            # adding the post engagement to the profile total engagement in the last month
            if post['likes_count'] is not None:
                profile_engagement_during_last_month += post['likes_count']
            if post['comments_count'] is not None:
                profile_engagement_during_last_month += post['comments_count']

        save_profile_with_engagement_in_db(profile, len(profile_posts_lst), profile_engagement_during_last_month)


def read_sqlite_table(table_name, db_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)

    # Read the table into a DataFrame
    df = pd.read_sql_query(f"SELECT ID, Name, Nickname, Bio, Post_Count, Follower_Count, Following_Count, Is_Business, Is_Private, Is_Verified FROM {table_name}", conn)

    # Close the database connection
    conn.close()

    return df


def main():
    db_file = '../../../api-data/profiles/profiles.db'
    nfl_users = read_sqlite_table('NFL_Profiles', db_file)

    calculate_profiles_engagement_store_in_db(nfl_users[THRESHOLD:])


if __name__ == "__main__":
    main()
