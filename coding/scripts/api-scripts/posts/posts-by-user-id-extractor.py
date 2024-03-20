from coding.APIConnectors.Data365Connector import Data365Connector
from coding.utility.InstagramAPIDatabaseHandler import InstagramAPIDatabaseHandler
from coding.loggers.Logger import logger
from datetime import datetime, timedelta
import pandas as pd

# ---------------- URLs ---------------------------
input_profiles_files_url = "../../../../data/api-data/profiles/NFL/seed-NFL-users-profile.csv"
posts_db_url = "../../../../data/api-data/posts-comments-replies/posts.db"
# ---------------- Objects ------------------------
api_connector = Data365Connector()
database_connector = InstagramAPIDatabaseHandler(posts_db_url)
# ---------------- CONSTS -------------------------
TO_DATE = (datetime.now() - timedelta(days=3)).date()
FROM_DATE = (TO_DATE - timedelta(days=14))
MAX_POSTS_TO_FETCH = 500
TABLE_NAME = 'snowball_posts'
# --------------- Data Structure ------------------


def get_profiles_id_list():
    # reading profiles dataframe
    profiles_df = pd.read_csv(input_profiles_files_url)
    # extracting and returning profiles id
    return list(profiles_df["ID"])


def main():
    logger.info("Storing profiles posts in given period process has started ...")
    # reading profiles id from input file
    profiles_id_list = get_profiles_id_list()

    # scanning all profiles id to fetch posts
    for idx, profile_id in enumerate(profiles_id_list):
        logger.info("Fetching profile "+str(profile_id)+" posts. ("+str(idx + 1)+"/"+str(len(profiles_id_list))+")")
        # fetching posts using api connector
        posts_list = api_connector.get_posts_by_profile_id(profile_id, MAX_POSTS_TO_FETCH, FROM_DATE, TO_DATE)
        logger.info(str(len(posts_list))+" posts were found that was posted by profile "+str(profile_id)+" in given period")
        # storing all posts in database
        for post in posts_list:
            # converting post json into tuple
            post_tuple = api_connector.get_post_data_tuple_from_json(post)
            # storing the post in the database
            database_connector.save_post_to_database(TABLE_NAME, post_tuple)


if __name__ == "__main__":
    main()
