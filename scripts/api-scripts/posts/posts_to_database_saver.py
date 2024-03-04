import sqlite3
import logging
import json

# -------------- URLS ----------------------------------------
posts_file_url = "../../../api-data/NFL/NFL-posts.csv"
# -------------- database objects ----------------------------
posts_data_db_url = "../../../api-data/instagram-posts.db"
connector = sqlite3.connect(posts_data_db_url)
cursor = connector.cursor()

# ---------------- logging objects ---------------------------
formatter = logging.Formatter(fmt="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
                              datefmt='%d-%m-%Y %H:%M:%S')
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)
screen_handler = logging.StreamHandler()
screen_handler.setFormatter(formatter)
logger.addHandler(screen_handler)


def init_instagram_posts_table():
    """
    a function to start a new user's profiles table in the database
    """
    cursor.execute("""CREATE TABLE IF NOT EXISTS instagram_posts (
                                id text PRIMARY KEY,
                                caption text,
                                owner_id text,
                                likes_count integer,
                                comments_count integer,
                                publication_timestamp text,
                                location_id text)""")
    connector.commit()
    logger.info("instagram posts table was created successfully")


def get_posts_data_from_file():
    logger.info("extracting posts data from posts data source.")
    with open(posts_file_url, "r") as posts_file:
        logger.debug("posts file was opened successfully.")
        posts_json_list = posts_file.read().splitlines()
        logger.debug("done reading file content successfully.")
    posts_json_list = [json.loads(json_str) for json_str in posts_json_list]
    logger.debug('posts data list was created successfully.')

    return posts_json_list


def save_posts_data_to_database(posts_data):
    logger.info("inserting posts data into the database.")
    for post in posts_data:
        post_id = post['id']
        logger.debug("saving post" + str(post_id) + "into the database")

        if post_id is None:
            logger.warning("Post ID is missing, could not insert into database")
        else:

            insert_query = '''
                            INSERT INTO instagram_posts (id, owner_id, caption, likes_count, comments_count, publication_timestamp, location_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            '''
            try:
                # Execute the SQL query with the provided parameters
                cursor.execute(insert_query,
                               (str(post_id),
                                str(post.get('owner_id', ' ')),
                                post.get('text', ' '),
                                post.get('likes_count', -1),
                                post.get('comments_count', -1),
                                post.get('timestamp', ' '),
                                post.get('location_id', ' ')))

                connector.commit()

            except sqlite3.IntegrityError as e:
                logger.warning(
                    "insertion was failed, post " + str(post_id) + " already exists in the database -Primary key.")
                connector.rollback()  # Rollback the transaction

    logger.info("done inserting all posts data into database")


def main():
    init_instagram_posts_table()
    posts_data = get_posts_data_from_file()
    save_posts_data_to_database(posts_data)


if __name__ == '__main__':
    main()
