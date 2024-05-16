from coding.APIConnectors.Data365Connector import Data365Connector
from coding.utility.InstagramAPIDatabaseHandler import InstagramAPIDatabaseHandler
from coding.loggers.Logger import logger
from datetime import datetime, timedelta
import pandas as pd

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
MAX_AMOUNT = 100
# Determining the minimum posting date
FROM_DATE = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
WANTED_PERIOD = "month"
# table to store profiles in order to snowball them
SNOWBALLED_PROFILES_ID_TABLE_NAME = "climate_change_profiles_id"
# table to store profiles after snowballing them (engagement calculated)
SNOWBALLED_PROFILES_ENGAGEMENT_TABLE_NAME = "climate_change_with_engagement_profiles"

# a variable to use if program crash in the middle
THRESHOLD = 0

# ---------------- FLAGS -----------------
# Turn on the flag if storing the initial profile seed into the database is desired.
STORE_SEED_IN_DB = False

"""
def calculate_profiles_engagement_store_in_db(profiles_df):
    for idx, profile in profiles_df.iterrows():
        profile_id = profile['ID']
        profile_engagement_during_last_month = 0

        logger.info("calculating profile with " + str(profile_id) + " id engagement. (" + str(idx) + "/" + str(
            len(profiles_df)+THRESHOLD) + ")")
        # fetching all profile posts from last month
        profile_posts_lst = api_connector.get_posts_by_profile_id(profile_id, MAX_AMOUNT, FROM_DATE)

        # scanning all posts to discover who interacted with same profile id
        for post in profile_posts_lst:
            # adding the post engagement to the profile total engagement in the last month
            if post['likes_count'] is not None:
                profile_engagement_during_last_month += post['likes_count']
            if post['comments_count'] is not None:
                profile_engagement_during_last_month += post['comments_count']

        save_profile_with_engagement_in_db(profile, len(profile_posts_lst), profile_engagement_during_last_month)
"""


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


def find_profiles_from_posts_comments_calculate_profile_engagement(profile, profile_posts_lst):
    """
    input: - the current profile which is being snowballed
           - current profiles posts list

    the function scan profiles posts in order to find new instagram users that interacted with the current profile.
    """
    # profile engagement is the number of likes and comments his posts has
    profile_engagement_during_period = 0

    # scanning all posts to discover who interacted with same profile id
    logger.info(f"Iterating over {profile['Name']} post comments to identify Instagram users who interacted with {profile['Name']}.")
    for j, post in enumerate(profile_posts_lst):
        post_id = post['id']
        logger.info(f"identifying profiles that commented on the post with ID 123. ({j+1}/{len(profile_posts_lst)}")

        # adding the post engagement to the profile total engagement in the last month
        if post['likes_count'] is not None:
            profile_engagement_during_period += post['likes_count']
        if post['comments_count'] is not None:
            profile_engagement_during_period += post['comments_count']

        logger.debug(f"Fetching post {post_id} comment's which was posted by {profile['Name']}")
        comments_lst = api_connector.get_post_comments(post_id, FROM_DATE, MAX_AMOUNT)

        # scanning comments to find new profiles that interacted with current profile
        new_profiles_found_from_post_count = find_store_new_profiles_from_comment(comments_lst)

        logger.info(f"{new_profiles_found_from_post_count} New profiles were found while scanning post {post_id} comments")
    
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

        if profile is not None and profile_posts_lst is not None:
            logger.info(f"Snowballing {profile['Name']} ({profile['Nickname']}) with profile id {profile['ID']}.\n"
                        f"{len(profile_posts_lst)} Posts were posted by {profile['Name']} ({profile['Nickname']}) in the last {WANTED_PERIOD}.")

            # scanning profile posts to calculate engagement and find new profiles
            profile_engagement_during_period = find_profiles_from_posts_comments_calculate_profile_engagement(profile, profile_posts_lst)

            # saving profile with engagement data in a database
            profile_tuple = api_connector.get_profile_data_tuple_from_json(profile_json)
            profiles_database_connector.save_profile_with_engagement_to_database(SNOWBALLED_PROFILES_ENGAGEMENT_TABLE_NAME,
                                                                                 profile_tuple + (len(profile_posts_lst), profile_engagement_during_period, ))

        # checking if we passed the wanted amount of snowballed profiles
        if snowballed_profiles_id_counter >= TO_SNOWBALL_AMOUNT:
            # we reached the amount we wanted, we stop the process
            break


def get_next_iteration_of_profiles_to_snowball():
    """
    the function reads both database tables:
        - profiles with engagement table (already snowballed profiles)
        - profiles id table (all profiles id)

    the function returns (profiles id) - (profiles id with engagement) list, which include all the profile's id we didn't snowball yet.
    """
    global already_found_profiles_id_set
    global snowballed_profiles_id_counter

    # reading databases tables as pandas dataframe
    profiles_id_df = profiles_database_connector.get_profiles_without_engagement_table_content_as_df(SNOWBALLED_PROFILES_ID_TABLE_NAME)
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
    if STORE_SEED_IN_DB:
        # inserting initial seed into database in order to start snowballing
        init_database_tables_store_seed_in_profiles_database()
        # initializing an empty profile with engagement table (required to start snowball)

    # starting snowball process until we have requested number of profiles
    while snowballed_profiles_id_counter < TO_SNOWBALL_AMOUNT:
        # Retrieving Instagram profiles not yet activated for a new snowball iteration
        profiles_id_to_snowball_list = get_next_iteration_of_profiles_to_snowball()
        # snowballing to get new list of profiles id
        snowball(profiles_id_to_snowball_list)
        
    # calculating the engagement of profiles that were found in the last iteration (we didn't snowball to discover engagement)
    # calculate_profiles_engagement_store_in_db(profiles_dfs_lst[SNOWBALL_ITERATIONS])


if __name__ == "__main__":
    main()
