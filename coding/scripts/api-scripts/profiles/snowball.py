from coding.APIConnectors.Data365Connector import Data365Connector
from coding.utility.InstagramAPIDatabaseHandler import InstagramAPIDatabaseHandler
from coding.loggers.Logger import logger
from datetime import datetime, timedelta
import pandas as pd

# ---------------- FLAGS -----------------
# Turn on the flag if storing the initial profile seed into the database is desired.
STORE_SEED_IN_DB = False
# if flag is false, the script will collect data for existing id's only
SNOWBALL_NEW_PROFILES = True

# ---------------- URLs ---------------------------
# csv file that stores initial profile's id (seed)
init_profiles_seed_file_url = "./../../../../data/seeds/climate-change.csv"
profiles_db_url = "./../../../../data/api-data/profiles/profiles.db"
posts_db_url = "./../../../../data/api-data/posts-comments-replies/posts.db"

# ---------------- Objects ------------------------
api_connector = Data365Connector()
profiles_database_connector = InstagramAPIDatabaseHandler(profiles_db_url)
posts_database_connector = InstagramAPIDatabaseHandler(posts_db_url)

# ----- GLOBAL PARAMETERS AND DATA STRUCTURE ------
snowballed_profiles_id_counter = 0
already_found_profiles_id_set = set()

# ---------------- CONSTS -------------------------
# amount of profiles to snowball
TO_SNOWBALL_AMOUNT = 100000
# max amount of posts to fetch from each profile
MAX_AMOUNT = 1000
# Determining the minimum posting date
FROM_DATE = (datetime.now() - timedelta(days=100)).strftime('%Y-%m-%d')
WANTED_PERIOD = "three months"
# database tables names
# table to store profiles in order to snowball them
SNOWBALLED_PROFILES_ID_TABLE_NAME = "climate_change_profiles_id"
# table to store profiles after snowballing them (engagement calculated)
SNOWBALLED_PROFILES_ENGAGEMENT_TABLE_NAME = "climate_change_with_engagement_profiles"
# table to store posts scanned during snowball process
POSTS_TABLE_NAME = "climate_change_snowball_posts"

# a variable to use if program crash in the middle
THRESHOLD = 0


def delete_database_tables():
    """
    the function deletes all the database tables this script create (cleaning)
    !!! use this function with caution !!!
    """
    profiles_database_connector.delete_database_table(SNOWBALLED_PROFILES_ENGAGEMENT_TABLE_NAME)
    profiles_database_connector.delete_database_table(SNOWBALLED_PROFILES_ID_TABLE_NAME)
    posts_database_connector.delete_database_table(POSTS_TABLE_NAME)


def find_store_new_profiles_from_comment(comments_lst):
    """
    input: - comments list

    the function scans the comments, fetch data about the comment owner id and store it in database
    """
    global snowballed_profiles_id_counter
    global already_found_profiles_id_set
    profiles_found_from_current_post = 0

    for comment in comments_lst:
        comment_owner_id = comment['owner_id']

        # checking if the profile id wasn't found before
        if comment_owner_id not in already_found_profiles_id_set:
            logger.debug(f"New profile with id {comment_owner_id} was found during snowball process")
            # saving new id into snowballed profiles id database table
            profiles_database_connector.save_id_to_database(SNOWBALLED_PROFILES_ID_TABLE_NAME, comment_owner_id)
            # updating counters and data structures
            profiles_found_from_current_post += 1
            snowballed_profiles_id_counter += 1
            already_found_profiles_id_set.add(comment_owner_id)

    return profiles_found_from_current_post


def fetch_profiles_data_calculate_engagement(profiles_id_list):
    """
    input: - profiles id list

    the function iterate over the profile's id, retrieve data, calculate engagement and store the data in database
    """
    for idx, profile_id in enumerate(profiles_id_list):
        (profile_json, profile_posts_lst) = api_connector.get_profile_data_and_posts_list_by_profile_id(profile_id, MAX_AMOUNT, FROM_DATE)
        profile_data_dict = api_connector.get_profile_data_dict_from_json(profile_json)
        profile_engagement_during_period = 0

        if profile_json is not None and len(profile_posts_lst) != 0:

            logger.debug(f"calculating engagement and storing data for profile {profile_data_dict['Nickname']} ({profile_data_dict['Nickname']}). ({idx}/{len(profiles_id_list)})")
            # scanning all posts to discover who interacted with same profile id
            for post in profile_posts_lst:
                # storing post data in database
                post_tuple = api_connector.get_post_data_tuple_from_json(post)
                posts_database_connector.save_post_to_database(POSTS_TABLE_NAME, post_tuple)

                # adding the post engagement to the profile total engagement in the last month
                if post['likes_count'] is not None:
                    profile_engagement_during_period += post['likes_count']
                if post['comments_count'] is not None:
                    profile_engagement_during_period += post['comments_count']

            # storing data in database
            profile_tuple = api_connector.get_profile_data_tuple_from_json(profile_json)
            profiles_database_connector.save_profile_with_engagement_to_database(SNOWBALLED_PROFILES_ENGAGEMENT_TABLE_NAME, profile_tuple + (len(profile_posts_lst), profile_engagement_during_period,))


def scan_profile_posts_fetch_comments_calculate_engagement(profile, profile_posts_lst):
    """
    input: - the current profile which is being snowballed
           - current profiles posts list

    the function scan profiles posts in order to find new instagram users that interacted with the current profile.
    """
    # profile engagement is the number of likes and comments his posts has
    profile_engagement_during_period = 0

    # scanning all posts to discover who interacted with same profile id
    logger.debug(f"Iterating over {profile['Nickname']} post comments to identify Instagram users who interacted with {profile['Nickname']}.")
    for j, post in enumerate(profile_posts_lst):
        # adding the post engagement to the profile total engagement in the last month
        if post['likes_count'] is not None:
            profile_engagement_during_period += post['likes_count']
        if post['comments_count'] is not None:
            profile_engagement_during_period += post['comments_count']
            logger.debug(f"Fetching post {post['id']} comment's which was posted by {profile['Nickname']}")

            # fetching post comments only if the post has comments
            if post['comments_count'] > 0:
                # storing post into database table (only if it has comments)
                post_data_tuple = api_connector.get_post_data_tuple_from_json(post)
                posts_database_connector.save_post_to_database(POSTS_TABLE_NAME, post_data_tuple)

                comments_lst = api_connector.get_post_comments(post['id'], FROM_DATE, MAX_AMOUNT)

                # checking if we got a non-empty comments list
                if len(comments_lst) != 0:
                    logger.info(
                        f"identifying profiles that commented on the post with ID {post['id']} which was was posted by {profile['Nickname']}. (post {j + 1} out of {len(profile_posts_lst)})")
                    # scanning comments to find new profiles that interacted with current profile
                    new_profiles_found_from_post_count = find_store_new_profiles_from_comment(comments_lst)
                    logger.info(
                        f"{new_profiles_found_from_post_count} New profiles were found while scanning post {post['id']} comments")
                    logger.info(
                        f">>>> Currently, [{snowballed_profiles_id_counter}] profiles were found during snowball <<<<")



    return profile_engagement_during_period


def snowball(profiles_id_list):
    """
    input: - profiles id to snowball

    the function iterate on the profile's id, collect data about profile using instagram API and snowball all the instagram users that interacted with each profile.
    """

    # scanning profile's ids to snowball
    for profile_id in profiles_id_list:
        (profile_json, profile_posts_lst) = api_connector.get_profile_data_and_posts_list_by_profile_id(profile_id, MAX_AMOUNT, FROM_DATE)
        profile = api_connector.get_profile_data_dict_from_json(profile_json)

        if profile is not None and len(profile_posts_lst) != 0:
            logger.info(f"Snowballing profile {profile['Nickname']} with id {profile['ID']}. ")
            logger.info(f"[[[{len(profile_posts_lst)} Posts were posted by {profile['Nickname']} in the last {WANTED_PERIOD}]]]")

            # scanning profile posts to calculate engagement and find new profiles
            profile_engagement_during_period = scan_profile_posts_fetch_comments_calculate_engagement(profile, profile_posts_lst)

            # saving profile with engagement data in a database
            profile_tuple = api_connector.get_profile_data_tuple_from_json(profile_json)
            profiles_database_connector.save_profile_with_engagement_to_database(SNOWBALLED_PROFILES_ENGAGEMENT_TABLE_NAME,
                                                                                 profile_tuple + (len(profile_posts_lst), profile_engagement_during_period, ))

        # checking if we passed the wanted amount of snowballed profiles
        if snowballed_profiles_id_counter >= TO_SNOWBALL_AMOUNT:
            # we reached the amount we wanted, we stop the process
            break


def get_next_iteration_of_profiles():
    """
    the function reads both database tables:
        - profiles with engagement table (already snowballed profiles)
        - profiles id table (all profiles id)

    the function returns (profiles id) - (profiles id with engagement) list, which include all the profile's id we didn't snowball yet.
    """
    global already_found_profiles_id_set
    global snowballed_profiles_id_counter

    # reading databases tables as pandas dataframe
    profiles_id_df = profiles_database_connector.get_id_table_content_as_df(SNOWBALLED_PROFILES_ID_TABLE_NAME)
    snowballed_profiles_df = profiles_database_connector.get_profiles_with_engagement_table_content_as_df(SNOWBALLED_PROFILES_ENGAGEMENT_TABLE_NAME)
    # extracting all the profiles that appear in SNOWBALLED_PROFILES_TABLE and doesn't appear in SNOWBALLED_PROFILES_ENGAGEMENT_TABLE
    to_snowball_profiles = profiles_id_df[~profiles_id_df.ID.isin(snowballed_profiles_df.ID)]
    # storing all the already found profiles id in a set and updating the found profiles counter
    already_found_profiles_id_set = set(profiles_id_df["ID"])
    snowballed_profiles_id_counter = len(already_found_profiles_id_set)

    return list(to_snowball_profiles["ID"])


def init_database_tables_store_seed_in_profiles_database():
    """
    the function reads the initial profile's id seeds from a prepared csv file,
    create new database tables to work with and stores the initial profiles in database profiles table to snowball them.
    """
    logger.info("Storing init profiles seed into profile's id database...")
    # reading csv file
    init_profiles_df = pd.read_csv(init_profiles_seed_file_url)

    # scanning the list of profiles id in order to store them in profiles to snowball database table
    for index, row in init_profiles_df.iterrows():
        # storing profile data in database
        profiles_database_connector.save_id_to_database(SNOWBALLED_PROFILES_ID_TABLE_NAME, row["id"])

    # initializing profiles with engagement table.
    profiles_database_connector.create_new_profile_engagement_table_in_db(SNOWBALLED_PROFILES_ENGAGEMENT_TABLE_NAME)


def main():
    snowball_iterations = 1

    if STORE_SEED_IN_DB:
        # inserting initial seed into database in order to start snowballing
        init_database_tables_store_seed_in_profiles_database()

    # starting snowball process until we have requested number of profiles
    profiles_id_to_snowball_list = get_next_iteration_of_profiles()

    while snowballed_profiles_id_counter < TO_SNOWBALL_AMOUNT and SNOWBALL_NEW_PROFILES:
        logger.info(
            f"Snowball iteration {snowball_iterations} has began, {TO_SNOWBALL_AMOUNT - snowballed_profiles_id_counter} profiles left to collect.")
        # Retrieving Instagram profiles not yet activated for a new snowball iteration
        # snowballing to get new list of profiles id
        snowball(profiles_id_to_snowball_list)
        profiles_id_to_snowball_list = get_next_iteration_of_profiles()

    logger.info("Snowball process has ended, retrieving profiles data for remaining profiles id")
    # retrieving data and calculating engagement for all the remaining profiles id
    fetch_profiles_data_calculate_engagement(profiles_id_to_snowball_list)


if __name__ == "__main__":
    main()
