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
FROM_DATE = (datetime.now() - timedelta(days=14)).strftime('%Y-%m-%d')
TO_DATE = datetime.now().date().strftime('%Y-%m-%d')
MAX_POSTS_TO_UPDATE = 500
NUMBER_OF_PAGES = 10
TABLE_NAME = 'posts_by_hashtag'
# --------------- Data Structure ------------------
hashtag_list = ['Buffalo Bills', 'Miami Dolphins', 'New England Patriots', 'New York Jets', 'Dallas Cowboys', 'New York Giants', 'Philadelphia Eagles', 'Washington Commanders',
                'Baltimore Ravens', 'Cincinnati Bengals', 'Cleveland Browns', 'Pittsburgh Steelers', 'Chicago Bears', 'Detroit Lions', 'Green Bay Packers', 'Minnesota Vikings',
                'Houston Texans', 'Indianapolis Colts',  'Jacksonville Jaguars',  'Tennessee Titans', 'Atlanta Falcons', 'Carolina Panthers', 'New Orleans Saints', 'Tampa Bay Buccaneers',
                'Denver Broncos', 'Kansas City Chiefs', 'Las Vegas Raiders', 'Los Angeles Chargers', 'Arizona Cardinals', 'Los Angeles Rams', 'San Francisco 49ers', 'Seattle Seahawks']


def main():
    short_hashtag_list = hashtag_list[10:]

    logger.info("Searching posts by hashtag process has began...")
    for hashtag in short_hashtag_list:
        # Sending request to get posts by hashtag
        logger.info("Searching for posts that include "+hashtag.lower().replace(" ", "")+" hashtag")
        
        json_posts_lst = api_connector.get_posts_by_hashtag(hashtag.lower().replace(" ", ""), MAX_POSTS_TO_UPDATE, FROM_DATE, TO_DATE, NUMBER_OF_PAGES)
        # saving received posts to database
        logger.info("Saving posts that include "+hashtag+" hashtag to database \n")

        for post_json in json_posts_lst:
            # converting post json into tuple (including hashtag too).
            post_tuple = api_connector.get_post_data_tuple_from_json(post_json) + (hashtag,)

            database_connector.save_post_to_database(TABLE_NAME, post_tuple, by_hashtag=True)


if __name__ == "__main__":
    main()
