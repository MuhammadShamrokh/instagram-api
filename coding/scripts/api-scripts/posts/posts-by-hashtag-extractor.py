from coding.APIConnectors.Data365Connector import Data365Connector
from coding.utility.InstagramAPIDatabaseHandler import InstagramAPIDatabaseHandler
from coding.loggers.Logger import logger
from datetime import datetime, timedelta

# ---------------- URLs ---------------------------
posts_db_url = "../../../../data/api-data/posts-comments-replies/posts.db"
# ---------------- Objects ------------------------
api_connector = Data365Connector()
database_connector = InstagramAPIDatabaseHandler(posts_db_url)
# ---------------- CONSTS -------------------------
hashtag_list = ['NFL']
FROM_DATE = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
TO_DATE = datetime.now()
MAX_POSTS = 5


def main():
    logger.info("Searching posts by hashtag process has began...")
    for hashtag in hashtag_list:
        logger.info("Searching for posts that include "+hashtag+" hashtag")
        # trying to get cached posts for now
        posts_lst = api_connector.get_cached_hashtag_posts(hashtag, FROM_DATE)

        logger.info("Saving posts that include "+hashtag+" hashtag to database")
        # saving received posts to database
        table_name = hashtag + "_Posts"
        for post in posts_lst:
            database_connector.save_post_to_database(table_name, post)


if __name__ == "__main__":
    main()
