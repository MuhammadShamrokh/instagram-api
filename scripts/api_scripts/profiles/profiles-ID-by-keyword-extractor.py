import json
import logging
import sqlite3
import requests
import time


# -------------------- external files url ---------------
user_profiles_db_url = "../../api-data/user-profiles.db"
user_profiles_full_data_file_url = "../../api-data/user-profiles-full-json.csv"

# --------------- global variables ---------------------
profiles_to_search_list = [
    "Liam", "Emma", "Noah", "Olivia", "Ava", "Isabella", "Sophia", "Jackson", "Aiden", "Lucas",
    "Harper", "Evelyn", "Abigail", "Amelia", "Mia", "Benjamin", "Elijah", "Henry", "Alexander", "James",
    "Scarlett", "Grace", "Lily", "Samuel", "Ethan", "Aria", "Ellie", "Zoe", "Layla", "Chloe",
    "Penelope", "Mason", "Addison", "Aubrey", "Emily", "Scarlett", "Natalie", "Hannah", "Lucy", "Paisley",
    "Wyatt", "Oliver", "Daniel", "Joseph", "Michael", "Emma", "Nicholas", "Logan", "Grace", "Anthony",
    "Victoria", "Isaac", "Evelyn", "Dylan", "Zoey", "Audrey", "Madison", "Ava", "Riley", "Sofia",
    "Julia", "Jack", "Eva", "Aiden", "Samuel", "Sophia", "Ethan", "Jackson", "Harper", "Scarlett",
    "Mia", "Amelia", "Aria", "Isabella", "Grace", "Lily", "Lucas", "Mason", "Chloe", "Benjamin",
    "Oliver", "Henry", "Amelia", "Elijah", "Abigail", "Alexander", "Charlotte", "Emily", "James", "Daniel",
    "William", "Michael", "Sophia", "Olivia", "Ava", "Isabella", "Mia", "Abigail", "Emily", "Harper",
    "Evelyn", "Scarlett", "Grace", "Chloe", "Amelia", "Aria", "Lily", "Zoey", "Addison", "Layla",
    "Riley", "Sophia", "Jackson", "Olivia", "Emma", "Liam", "Noah", "Ava", "Isabella", "Sophia",
    "Mia", "Amelia", "Harper", "Evelyn", "Abigail", "Ella", "Scarlett", "Grace", "Lily", "Chloe",
    "Aria", "Zoey", "Charlotte", "Riley", "Aiden", "Jackson", "Lucas", "Liam", "Noah", "Elijah",
    "Oliver", "Mason", "Logan", "James", "Benjamin", "Henry", "Alexander", "Ethan", "Wyatt", "Samuel",
    "Owen", "Joseph", "Daniel", "David", "Matthew", "Carter", "Jack", "Nicholas", "Gabriel", "Julian",
    "Ryan", "Jaxon", "Isaiah", "Levi", "Nathan", "Caleb", "Hunter", "Christian", "Eli", "Sebastian",
    "Cooper", "Brayden", "Lincoln", "Landon", "Jonathan", "Isaac", "Colton", "Carson", "Dylan", "Justin",
    "Dominic", "Austin", "Jordan", "Adam", "Ian", "Evan", "Aaron", "Caleb", "Zachary", "Thomas",
    "Charles", "Joel", "Anthony", "Nathaniel", "Xavier", "Jace", "Hayden", "Kevin", "Parker", "Chase",
    "Albert Einstein", "Marilyn Monroe", "Martin Luther King Jr.", "Oprah Winfrey", "Elvis Presley",
    "Steve Jobs", "Leonardo da Vinci", "Michael Jackson", "Mother Teresa", "Nelson Mandela",
    "Princess Diana", "Walt Disney", "John F. Kennedy", "Mahatma Gandhi", "George Washington",
    "Abraham Lincoln", "William Shakespeare", "Amelia Earhart", "Malala Yousafzai", "Muhammad Ali",
    "Queen Elizabeth II", "Steve Irwin", "David Bowie", "Bob Marley", "Bill Gates", "Stephen Hawking",
    "Marie Curie", "Charles Darwin", "Winston Churchill", "Pablo Picasso", "Vincent van Gogh",
    "Frida Kahlo", "Michael Jordan", "Serena Williams", "Usain Bolt", "Beyoncé", "Elton John",
    "Angelina Jolie", "Brad Pitt", "Leonardo DiCaprio", "Johnny Depp", "Tom Hanks", "Meryl Streep",
    "Emma Watson", "George Clooney", "Jennifer Aniston", "Kanye West", "Taylor Swift", "David Beckham",
    "Cristiano Ronaldo", "Lionel Messi", "Roger Federer", "Serena Williams", "Adele", "Ellen DeGeneres",
    "Oscar Wilde", "J.K. Rowling", "Stephen King", "Mark Twain", "Maya Angelou", "F. Scott Fitzgerald",
    "Emily Dickinson", "Pablo Neruda", "Haruki Murakami", "Agatha Christie", "Stephen Spielberg",
    "Quentin Tarantino", "Christopher Nolan", "Meryl Streep", "Robert De Niro", "Tom Cruise", "Kate Winslet",
    "Cate Blanchett", "Anthony Hopkins", "Morgan Freeman", "Daniel Day-Lewis", "Eddie Murphy",
    "Robin Williams", "Meryl Streep", "Julia Roberts", "Charlize Theron", "Denzel Washington", "Tom Hardy",
    "Angelina Jolie", "Natalie Portman", "Jennifer Lawrence", "Scarlett Johansson", "Emma Stone",
    "Brad Pitt", "Leonardo DiCaprio", "Johnny Depp", "Keanu Reeves", "Will Smith", "Dwayne Johnson",
    "Scarlett Johansson", "Robert Downey Jr.", "Jennifer Lopez", "Beyoncé", "Lady Gaga"
]

update_data_url = "https://api.data365.co/v1.1/instagram/search/profiles/update"
fetch_data_url = "https://api.data365.co/v1.1/instagram/search/profiles/items"
api_acess_token = "ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKSVV6STFOaUo5LmV5SnpkV0lpT2lKV2FXVnlZV3hzZVNJc0ltbGhkQ0k2TVRjd05UTXpNVEF4Tmk0NU1UY3lNVGcwZlEuZk1RMUFjS3FfSF9FWWNxY1M5aFZVNm1UbmMtWGlrWm9uZFdRcXpKOFZ3TQ=="

post_request_query_string = {"keywords": "", "max_profiles": 100, "access_token": api_acess_token}
get_status_request_query_string = {"keywords": "", "access_token": api_acess_token}
get_data_request_query_string = {"keywords": "", "max_page_size": 50, "access_token": api_acess_token}
# Configured to store the profiles of previous users' IDs in order to avoid saving the same profile twice in the database.
user_id_set = set()
profiles_search_count = 0
# --------------- ---------------- ---------------------

logging.basicConfig(level=logging.INFO)
connector = sqlite3.connect(user_profiles_db_url)
cursor = connector.cursor()


def init_user_profiles_table():
    """
    a function to start a new user's profiles table in the database
    """
    cursor.execute("""CREATE TABLE IF NOT EXISTS user_profiles (
                                ID text PRIMARY KEY,
                                Name text,
                                Nickname text)""")
    connector.commit()
    logging.info("user profiles table table was created successfully")


def save_profiles_to_database(data_response_body_dict):
    profiles_list = list(data_response_body_dict['data']['items'])
    for profile_data in profiles_list:
        (profile_id, name, nickname) = get_profile_data(profile_data)
        if profile_id is None:
            logging.warning(nickname + " ID is missing, could not insert into database")
        elif profile_id in user_id_set:
            logging.warning("insertion was failed, "+nickname + " profile id already exists in the database -ID set.")
        else:
            insert_query = '''
                    INSERT INTO user_profiles (ID, Name, Nickname)
                    VALUES (?, ?, ?)
                    '''
            try:
                # Execute the SQL query with the provided parameters
                cursor.execute(insert_query, (str(profile_id), name, nickname))
                connector.commit()
                logging.debug(nickname + " profile information was inserted to database successfully.")

            except sqlite3.IntegrityError as e:
                logging.warning("insertion was failed, "+nickname + " profile id already exists in the database -Primary key.")
                connector.rollback()  # Rollback the transaction


def get_profile_data(profile_data):
    profile_id = profile_data.get("id", None)

    name = profile_data.get("full_name", None)
    if name is None:
        name = "unknown"

    nickname = profile_data.get("username", None)
    if nickname is None:
        nickname = "unknown"

    return profile_id, name, nickname


def send_post_request(keyword):
    post_request_query_string["keywords"] = keyword
    update_response = requests.post(update_data_url, params=post_request_query_string)
    logging.debug("Post request was sent to start a profile search update task.")

    return update_response


def wait_for_database_update_task_to_finish(keyword):
    get_status_request_query_string["keywords"] = keyword
    # waiting for the post task to finish (using do while implementation in python)
    data_cached = False
    while True:
        status_response = requests.get(update_data_url, params=get_status_request_query_string)
        logging.debug("GET status request was sent.")
        # checking GET status request status code
        if status_response.status_code == 200:
            logging.debug("GET status request was sent successfully - response status code is 200.")
            status_response_body = json.loads(status_response.text)

            if status_response_body['data']['status'] == 'finished':
                logging.debug("The status of the profile search request is 'finished'.")
                data_cached = True
                break
            elif status_response_body['data']['status'] == 'fail':
                logging.warning("Post request status is 'fail'.")
                break
            else:
                # the profile search update task is not finished yet
                logging.debug("the profile search update task is not finished yet, current status is: " +
                              status_response_body['data']['status'])
                time.sleep(3)
        else:  # GET status request failed
            status_response_body_dict = json.loads(status_response.text)
            logging.warning("GET status request has failed, error: " + str(status_response_body_dict["error"]))
            break

    return data_cached


def send_get_data_request(keyword):
    # sending GET request to extract the data from the databases
    get_data_request_query_string['keywords'] = keyword
    get_data_response = requests.get(fetch_data_url, params=get_data_request_query_string)
    logging.debug("Request to fetch data was sent.")

    return get_data_response


def find_profiles_using_names_list():
    """
    the function searches instagram profiles using data365 Instagram API
    """
    global profiles_search_count

    logging.info("Searching profiles process started.")
    for keyword in profiles_to_search_list:
        logging.info("Searching for an Instagram profile using the keyword '" + keyword + "'.")
        update_response = send_post_request(keyword)

        # checking post request status code
        if update_response.status_code == 202:
            logging.debug("Post request was sent successfully - response status code is 202.")
            data_cached = wait_for_database_update_task_to_finish(keyword)

            # data was cached in API
            if data_cached:
                get_data_response = send_get_data_request(keyword)
                data_response_body_dict = json.loads(get_data_response.text)

                if get_data_response.status_code == 200:
                    logging.debug("GET data request was sent successfully - response status code is 200.")
                    save_profiles_to_database(data_response_body_dict)

                    logging.info("The Instagram profiles identified during the search with the keyword "+keyword+" have been successfully inserted into the database.")

                else:
                    logging.warning("GET data request has failed, error: " + str(data_response_body_dict["error"]))

        else:   # POST request has failed
            update_response_body_dict = json.loads(update_response.text)
            logging.warning("POST request has failed, error: " + str(update_response_body_dict["error"]))

        profiles_search_count = profiles_search_count + 1
        logging.info("Profiles search in process.... " + str(profiles_search_count) + "/" + str(len(profiles_to_search_list)))
        # wait 1 sec (API can't receive more than 100 requests per sec)
        time.sleep(0.1)

    connector.close()
    logging.info("Done searching and inserting user's profiles to database, Database connection was closed successfully")


def start_program():
    init_user_profiles_table()
    find_profiles_using_names_list()


if __name__ == "__main__":
    profiles_to_search_list = list(set(profiles_to_search_list))
    start_program()
