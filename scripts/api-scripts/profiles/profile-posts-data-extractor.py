import json
import logging
import pandas as pd
import requests
import time


# -------------------- external files url ---------------
profiles_data_input_file_url = "../../api-data/user_profiles.csv"
feed_posts_file_url = "../../api-data/feed_posts.csv"
# --------------- global variables ---------------------
api_url = "https://api.data365.co/v1.1/instagram/profile/"
fetch_data_url = "https://api.data365.co/v1.1/instagram/profile/{profile_id}/{section}/posts"
api_acess_token = "ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKSVV6STFOaUo5LmV5SnpkV0lpT2lKV2FXVnlZV3hzZVNJc0ltbGhkQ0k2TVRjd05UTXpNVEF4Tmk0NU1UY3lNVGcwZlEuZk1RMUFjS3FfSF9FWWNxY1M5aFZVNm1UbmMtWGlrWm9uZFdRcXpKOFZ3TQ=="

post_request_query_string = {"max_posts": 100, "from_date": "2024-01-06", "load_feed_posts": True, "load_tagged_posts": True, "load_reels_posts": True, "access_token": api_acess_token}
get_status_request_query_string = {"access_token": api_acess_token}
get_data_request_query_string = {"from_date": "2024-04-06", "order_by": "date_desc", "access_token": api_acess_token}
get_data_request_with_cursor_query_string = {"from_date": "2024-04-06", "order_by": "date_desc", "cursor": "", "access_token": api_acess_token}
# Configured to store the profiles of previous users' IDs in order to avoid saving the same profile twice in the database.
user_id_set = set()
# --------------- ---------------- ---------------------

logging.basicConfig(level=logging.DEBUG)


def get_users_profile_id_list():
    profiles_df = pd.read_csv(profiles_data_input_file_url)
    return list(profiles_df["ID"])


def send_post_request(profile_id):
    current_url = api_url + str(profile_id) + "/update"
    update_response = requests.post(current_url, params=post_request_query_string)
    logging.debug("Post request was sent to start a profile search update task.")

    return update_response


def wait_for_database_update_task_to_finish(profile_id):
    # waiting for the post task to finish (using do while implementation in python)
    data_cached = False
    current_url = api_url + str(profile_id) + "/update"
    while True:
        status_response = requests.get(current_url, params=get_status_request_query_string)
        logging.debug("GET status request was sent.")
        # checking GET status request status code
        if status_response.status_code == 200:
            logging.debug("GET status request was sent successfully - response status code is 200.")
            status_response_body = json.loads(status_response.text)

            if status_response_body['data']['status'] == 'finished':
                logging.debug("The status of the profile update request is 'finished'.")
                data_cached = True
                break
            elif status_response_body['data']['status'] == 'fail':
                logging.warning("Post request status is 'fail'.")
                break
            else:
                # the profile search update task is not finished yet
                logging.debug("the profile  update task is not finished yet, current status is: " +
                              status_response_body['data']['status'])
                time.sleep(0.5)
        else:  # GET status request failed
            status_response_body_dict = json.loads(status_response.text)
            logging.warning("GET status request has failed, error: " + str(status_response_body_dict["error"]))
            break

    return data_cached


def send_get_data_request(profile_id, cursor):
    current_url = api_url + str(profile_id) + "/feed/posts"
    # sending GET request to extract the data from the databases
    if cursor is None:
        get_data_response = requests.get(current_url, params=get_data_request_query_string)
    else:
        get_data_request_with_cursor_query_string["cursor"] = cursor
        get_data_response = requests.get(current_url, params=get_data_request_with_cursor_query_string)
    logging.debug("Request to fetch posts data was sent.")

    return get_data_response


def save_posts_data_to_file(posts_list):
    with open(feed_posts_file_url, 'a') as file:
        for post in posts_list:
            profile_json_str = json.dumps(post)
            file.write(profile_json_str + '\n')


def save_posts_data(profile_id):
    cursor = None
    # scanning all the data pages
    while True:
        get_data_response = send_get_data_request(profile_id, cursor)
        data_response_body_dict = json.loads(get_data_response.text)

        if get_data_response.status_code == 200:
            logging.debug("GET data request was sent successfully - response status code is 200.")
            posts_list = list(data_response_body_dict["data"]["items"])
            page_info = data_response_body_dict["data"]["page_info"]
            save_posts_data_to_file(posts_list)
            logging.debug("The page of posts information was saved")
            if page_info["has_next_page"]:
                logging.debug("Theres another page of posts information, going to send another get request.")
                # there is another page of posts data, we get the cursor to use in the next get request
                cursor = page_info["cursor"]
            else:
                logging.debug("This was the last page of posts information.")
                break
        else:
            logging.warning("GET data request has failed, error: " + str(data_response_body_dict["error"]))


def store_users_posts_in_file(profiles_list):
    profiles_count = 0
    logging.info("Posts fetching process has started.")
    for profile_id in profiles_list:
        logging.info("Getting data for instagram profile with id " + str(profile_id))
        update_response = send_post_request(profile_id)

        # checking post request status code
        if update_response.status_code == 202:
            logging.debug("Post request was sent successfully - response status code is 202.")
            data_cached = wait_for_database_update_task_to_finish(profile_id)

            # data was cached in API
            if data_cached:
                save_posts_data(profile_id)
                logging.info("Instagram profile " + profile_id + " posts were saved in posts file successfully.")

        else:  # POST request has failed
            update_response_body_dict = json.loads(update_response.text)
            logging.warning("POST request has failed, error: " + str(update_response_body_dict["error"]))

        profiles_count = profiles_count + 1
        logging.info(
            "Getting profiles data in process.... " + str(profiles_count) + "/" + str(len(profiles_list)))
        # wait 1 sec (API can't receive more than 100 requests per sec)
        time.sleep(0.1)


def start_program():
    profiles_id_list = get_users_profile_id_list()
    store_users_posts_in_file(profiles_id_list)


if __name__ == "__main__":
    start_program()
