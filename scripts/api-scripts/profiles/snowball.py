from utility.InstagramAPIDatabaseHandler import InstagramAPIDatabaseHandler
from APIConnectors.Data365Connector import Data365Connector
from datetime import datetime, timedelta
from loggers.Logger import logger
import pandas as pd

# ---------------- URLs ---------------------------
input_profiles_files_url = "../../../api-data/profiles/NFL/NFL-users-profile.csv"
user_profiles_db_url = "../../../api-data/profiles/profiles.db"
# ---------------- Objects ------------------------
api_connector = Data365Connector()
database_connector = InstagramAPIDatabaseHandler(user_profiles_db_url)
# ---------------- CONSTS -------------------------
SNOWBALL_ITERATIONS = 2
MAX_AMOUNT = 1000
# ---------------- Data structure -----------------
# a set to store all the new profiles that were found during snowball (set to prevent duplicates)
new_profiles_set = set()
# array of dataframes to store data of the new profile found in each snowball iteration



def read_profiles_id():
    origin_profiles_df = pd.read_csv(input_profiles_files_url)
    profiles_id_lst = list(origin_profiles_df['ID'])

    return profiles_id_lst[:]


def snowball(origin_profiles_list):
    current_snowball_new_profiles_set = set()

    # scanning all profiles to snowball
    for profile_id in origin_profiles_list:
        logger.debug("Snowballing profile with "+str(profile_id)+" id.")
        # fetching all profile posts from last month
        one_month_ago_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        profile_posts_lst = api_connector.get_profile_posts(profile_id, MAX_AMOUNT, one_month_ago_date)

        # scanning all posts to discover who interacted with same profile id
        for post in profile_posts_lst:
            post_id = post["id"]
            logger.debug("Fetching data for post " + str(post_id) + " who was posted by " + str(profile_id))
            comments_lst = api_connector.get_post_comments(post_id, one_month_ago_date, MAX_AMOUNT)

            # scanning comments to store owner id of each comment
            for comment in comments_lst:
                comment_owner_id = comment['owner_id']
                # checking if the profile id wasn't found before
                if comment_owner_id not in new_profiles_set:
                    current_snowball_new_profiles_set.add(comment_owner_id)
                    new_profiles_set.add(comment_owner_id)

    return current_snowball_new_profiles_set


def main():
    # read initial profiles
    initial_profiles_list = read_profiles_id()
    # snow ball to get bigger list of profiles
    snowball_result = snowball(initial_profiles_list)

    # get all data about profiles and store into a database
    for profile_id in snowball_result:
        database_connector.save_profile_to_database("NFL-Profiles", api_connector.get_profile_data_by_id(profile_id))


if __name__ == "__main__":
    main()

