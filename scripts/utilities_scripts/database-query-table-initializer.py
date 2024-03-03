import sqlite3
import logging

database_url = "../../api-data/user-profiles-full.db"

connector = sqlite3.connect(database_url)
cursor = connector.cursor()
logging.basicConfig(level=logging.INFO)


def start():
    cursor.execute("""CREATE TABLE active_users
                          AS
                          SELECT *
                          FROM user_profiles
                          WHERE Is_Private = 0 and Post_Count>= 1000""")
    connector.commit()
    logging.info("Table was created successfully!.")


if __name__ == "__main__":
    start()
