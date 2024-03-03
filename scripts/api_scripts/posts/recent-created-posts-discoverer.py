import os
import requests
from urllib.parse import urlparse
import json
import logging
import sys
import time
from datetime import datetime, timedelta

import pandas as pd

user_profiles_data_file_url = "../../../api-data/NFL/NFL-users-profile.csv"
new_posts_data_file_url ="../../api-data/NFL/recent-created-posts-31-1.csv"

# ------------------  API URLS  --------------------
update_and_status_profile_info_url_base = "https://api.data365.co/v1.1/instagram/profile/"
get_profile_posts_data_base_url = "https://api.data365.co/v1.1/instagram/profile/"

api_access_token = "ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKSVV6STFOaUo5LmV5SnpkV0lpT2lKV2FXVnlZV3hzZVNJc0ltbGhkQ0k2TVRjd05UTXpNVEF4Tmk0NU1UY3lNVGcwZlEuZk1RMUFjS3FfSF9FWWNxY1M5aFZVNm1UbmMtWGlrWm9uZFdRcXpKOFZ3TQ=="

# ------------------ QUERY PARAMETERS ---------------
profile_post_request_query_parameters = {"from_date": "", "load_feed_posts": True, "max_posts": 25, "load_comments": False, "load_replies": False, "access_token": api_access_token}
status_query_parameters = {"access_token": api_access_token}
get_request_query_parameters = {"from_date": "", "order_by": "date_desc", "max_page_size": 100, "access_token": api_access_token}
# --------------- Logger objects ---------------------

formatter = logging.Formatter(fmt="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
                              datefmt='%d-%m-%Y %H:%M:%S')
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(filename="../logs/new-posts-search-logs.log", mode='a')
screen_handler = logging.StreamHandler()
file_handler.setFormatter(formatter)
screen_handler.setFormatter(formatter)
logger.addHandler(screen_handler)
logger.addHandler(file_handler)


def get_user_id_list():
    try:
        profiles_df = pd.read_csv(user_profiles_data_file_url)
        profiles_id_list = list(profiles_df["ID"])
        ret_profiles_id = profiles_id_list[0:]
        profiles_name_list = list(profiles_df["Nickname"])
        ret_profiles_name = profiles_name_list[0:]
    except Exception as err:
        print(err)
        sys.exit(0)

    return ret_profiles_id, ret_profiles_name


def set_searching_from_date_query_parameter():
    # Getting the current date
    current_date = datetime.now()
    # Calculate yesterday's date
    search_start_date = current_date - timedelta(hours=5)
    # Format yesterday's date in ISO 8601 format
    iso_format_search_start = search_start_date.strftime('%Y-%m-%dT%H:%M:%S')

    # Updating Query parameter 'from_date'
    profile_post_request_query_parameters['from_date'] = iso_format_search_start
    get_request_query_parameters['from_date'] = iso_format_search_start

    print(profile_post_request_query_parameters)

def wait_for_database_update_task_to_finish(url, query):
    # waiting for the post task to finish (using do while implementation in python)
    data_cached = False

    try:
        while True:
            status_response = requests.get(url, params=query)
            logger.debug("GET status request was sent.")
            # checking GET status request status code
            if status_response.status_code == 200:
                logger.debug("GET status request was sent successfully - response status code is 200.")
                status_response_body = json.loads(status_response.text)

                if status_response_body['data']['status'] == 'finished':
                    logger.debug("The status of the profile update request is 'finished'.")
                    data_cached = True
                    break
                elif status_response_body['data']['status'] == 'fail' or status_response_body['data']['status'] == 'canceled':
                    logger.warning("Post request status is "+status_response_body['data']['status']+".")
                    break
                else:
                    # the profile search update task is not finished yet
                    logger.debug("the profile  update task is not finished yet, current status is: " + status_response_body['data']['status'])
                    time.sleep(5)
            else:  # GET status request failed
                status_response_body_dict = json.loads(status_response.text)
                logger.warning("GET status request has failed, error: " + str(status_response_body_dict["error"]))
                break
    except Exception as e:
        data_cached = False
        logger.warning("Something went wrong.... (line 115)")

    return data_cached


def search_for_new_posts():
    # getting list of stored NFL instagram profiles
    id_list, names_list = get_user_id_list()

    # setting script to search for posts that were created in the last 12 hours
    set_searching_from_date_query_parameter()

    # scanning list of stored NFL instagram profiles to seek for new created posts
    for idx, profile_id in enumerate(id_list):
        logger.info("Looking for "+names_list[idx]+" new posts.")

        # Creating POST request
        api_url = update_and_status_profile_info_url_base + str(profile_id) + "/update"
        update_response = requests.post(api_url, params=profile_post_request_query_parameters)

        if update_response.status_code == 202:
            logger.debug("Post request was sent successfully - response status code is 202.")
            data_cached = wait_for_database_update_task_to_finish(api_url, status_query_parameters)
            # data was cached in API
            if data_cached:
                # Creating GET request
                get_posts_data_url = get_profile_posts_data_base_url + str(profile_id) + "/feed/posts"
                posts_list = fetch_data_return_list("Posts", get_posts_data_url)



if __name__ == "__main__":
    search_for_new_posts()

