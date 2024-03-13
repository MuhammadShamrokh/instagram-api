from APIConnectors.Data365Connector import Data365Connector
from utility.InstagramAPIDatabaseHandler import InstagramAPIDatabaseHandler
from datetime import datetime, timedelta
from loggers.Logger import logger


# ---------------- URLs ---------------------------
posts_db_url = "../../../api-data/posts-comments-replies/posts.db"
# ---------------- Objects ------------------------
api_connector = Data365Connector()
database_connector = InstagramAPIDatabaseHandler(posts_db_url)
# ---------------- CONSTS -------------------------
hashtag_list = ['NFL']
FROM_DATE = datetime(2024, 2, 1).date()
TO_DATE = (FROM_DATE + timedelta(days=14)).strftime('%Y-%m-%d')
MAX_POSTS = 5000


def main():
    for hashtag in hashtag_list:
        posts_lst = api_connector.search_posts_by_hashtag(hashtag, MAX_POSTS, FROM_DATE, TO_DATE)

        for post in posts_lst:
            database_connector.save_post_to_database("NFL_Posts", post)





if __name__ == "__main__":
    main()
