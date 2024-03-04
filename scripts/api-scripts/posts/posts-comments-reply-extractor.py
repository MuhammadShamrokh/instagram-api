import json
import logging
import pandas as pd
import requests
import time
import sys
import os
import datetime

from APIConnectors.Data365Connector import Data365Connector
from scripts.api_scripts.Data365APIConnector import *

if not os.path.exists('../logs'):
    os.mkdir('../logs')

# # # # # -------------------- external files url ---------------
profiles_data_input_file_url = "../../../api-data/NFL/tests/test-profiles.csv"
posts_file_url = "../../../api-data/NFL/tests/NFL-test-posts.csv"
comments_file_url = "../../../api-data/NFL/tests/NFL-test-comments.csv"
replies_file_url = "../../../api-data/NFL/tests/NFL-test-replies.csv"

# # # # # --------------- API URL ---------------------
# posts data urls
update_and_status_profile_info_url_base = "https://api.data365.co/v1.1/instagram/profile/"
get_posts_data_url_base = "https://api.data365.co/v1.1/instagram/profile/"

# comments data urls
get_comments_data_url_base = "https://api.data365.co/v1.1/instagram/post/"

# replies data urls
get_replies_data_url_base = "https://api.data365.co/v1.1/instagram/comment/"

api_access_token = "ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKSVV6STFOaUo5LmV5SnpkV0lpT2lKV2FXVnlZV3hzZVNJc0ltbGhkQ0k2TVRjd05UTXpNVEF4Tmk0NU1UY3lNVGcwZlEuZk1RMUFjS3FfSF9FWWNxY1M5aFZVNm1UbmMtWGlrWm9uZFdRcXpKOFZ3TQ=="

# # # # # --------------- Query parameters dictionaries --------------------
profile_post_request_query_parameters = {"from_date": "2024-01-24", "load_feed_posts": True, "max_posts": 100, "load_comments": True, "max_comments": 1000, "load_replies": True, "access_token": api_access_token}
profile_status_request_query_parameters = {"access_token": api_access_token}
get_request_query_parameters = {"from_date": "2024-01-24", "order_by": "date_desc", "max_page_size": 100, "access_token": api_access_token}
get_request_with_cursor_query_parameters = {"from_date": "2024-01-24", "order_by": "date_desc", "max_page_size": 100, "cursor": "", "access_token": api_access_token}

# --------------- Logger objects ---------------------

formatter = logging.Formatter(fmt="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
                              datefmt='%d-%m-%Y %H:%M:%S')
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler(filename="../logs/NFL-api-reliability-test.log", mode='a')
screen_handler = logging.StreamHandler()
file_handler.setFormatter(formatter)
screen_handler.setFormatter(formatter)
logger.addHandler(screen_handler)
logger.addHandler(file_handler)
# ------------------------------------------------------


def get_users_profile_id_nickname_list():
    try:
        profiles_df = pd.read_csv(profiles_data_input_file_url)
        profiles_id_list = list(profiles_df["ID"])
        ret_profiles_id = profiles_id_list[0:]
        profiles_name_list = list(profiles_df["Nickname"])
        ret_profiles_name = profiles_name_list[0:]
    except Exception as err:
        print(err)
        sys.exit(0)

    return ret_profiles_id, ret_profiles_name


def save_data_list_to_file(data_list, file_url):
    with open(file_url, 'a') as file:
        for data in data_list:
            profile_json_str = json.dumps(data)
            file.write(profile_json_str + '\n')


def add_post_id_to_comment_dic(comments_lst, post_id):
    for comment in comments_lst:
        comment['post_id'] = post_id


def add_post_comment_id_to_reply_dic(replies_lst, post_id, comment_id):
    for reply in replies_lst:
        reply["post_id"] = post_id
        reply["comment_id"] = comment_id


def send_get_data_request(data_type, url, cursor):
    # sending GET request to extract the data from the databases
    if cursor is None:
        get_data_response = requests.get(url, params=get_request_query_parameters)
    else:
        get_request_with_cursor_query_parameters["cursor"] = cursor
        get_data_response = requests.get(url, params=get_request_with_cursor_query_parameters)
        logger.debug("Request to fetch "+data_type+" data was sent.")

    return get_data_response


def fetch_data_return_list(data_type, url):
    cursor = None
    data_list = list()
    pages_count = 0

    try:
        # scanning all the data pages
        while True:
            get_data_response = send_get_data_request(data_type, url, cursor)
            data_response_body_dict = json.loads(get_data_response.text)

            if get_data_response.status_code == 200:
                logger.debug("GET "+data_type+" request was sent successfully - response status code is 200.")
                data_list.extend(list(data_response_body_dict["data"]["items"]))
                page_info = data_response_body_dict["data"]["page_info"]
                pages_count = pages_count + 1
                logger.debug("The "+str(pages_count)+" page of "+data_type+" information was saved")
                if page_info["has_next_page"]:
                    logger.debug("Theres another page of "+data_type+" information, going to send another get request.")
                    # there is another page of posts data, we get the cursor to use in the next get request
                    cursor = page_info["cursor"]
                else:
                    logger.debug("This was the last page of "+data_type+" information.")
                    break
            else:
                logger.warning("GET data request has failed, error: " + str(data_response_body_dict["error"]))

            time.sleep(0.1)
    except Exception as e:
        logger.warning("Something went wrong. (line 161)")

    return data_list


def update_profile_get_posts_list(profile_id):
    posts_list = list()
    api_url = update_and_status_profile_info_url_base + str(profile_id) + "/update"

    update_response = requests.post(api_url, params=profile_post_request_query_parameters)
    # checking post request status code
    if update_response.status_code == 202:
        logger.debug("Post request was sent successfully - response status code is 202.")
        data_cached = wait_for_database_update_task_to_finish(api_url, profile_status_request_query_parameters, logger)
        # data was cached in API
        if data_cached:
            get_posts_data_url = get_posts_data_url_base + str(profile_id) + "/feed/posts"
            posts_list = fetch_data_return_list("Posts", get_posts_data_url)
    else:
        update_response_body_dict = json.loads(update_response.text)
        logger.warning("POST request has failed, error: " + str(update_response_body_dict["error"]))

    return posts_list


def store_posts_comments_replies_for_profiles(profiles_id_list, profiles_nicknames_list):
    logger.info("Fetching profile's posts,comments and replies process has started.")

    for index, profile_id in enumerate(profiles_id_list):
        start = datetime.datetime.now()
        logger.info("Fetching and saving profile "+profiles_nicknames_list[index]+" posts, comments and replies")
        posts_list = update_profile_get_posts_list(profile_id)
        save_data_list_to_file(posts_list, posts_file_url)

        for post in posts_list:
            if post["comments_count"] is not None and post["comments_count"] > 0:
                post_id = post["id"]
                get_comments_data_url = get_comments_data_url_base + str(post_id) + "/comments"
                comments_list = fetch_data_return_list("Comments", get_comments_data_url)
                add_post_id_to_comment_dic(comments_list, post_id)
                save_data_list_to_file(comments_list, comments_file_url)

                for comment in comments_list:
                    if comment["comments_count"] is not None and comment["comments_count"] > 0:
                        comment_id = comment["id"]
                        get_replies_data_url = get_replies_data_url_base + str(comment_id) + "/replies"
                        replies_list = fetch_data_return_list("replies", get_replies_data_url)
                        add_post_comment_id_to_reply_dic(replies_list, post_id, comment_id)
                        save_data_list_to_file(replies_list, replies_file_url)

        end = datetime.datetime.now()
        duration = int((end - start).total_seconds())
        logger.info("Done Fetching and saving profile " + profiles_nicknames_list[index] + " posts, comments and replies. duration: "+str(duration)+" seconds.")

        logger.info(
            "Getting profile posts, comments and replies data in process.... " + str(index + 1) + "/" + str(len(profiles_id_list)))
        # wait 1 sec (API can't receive more than 100 requests per sec)
        time.sleep(0.1)


def start_program():
    profiles_id_list, profiles_nicknames_list = get_users_profile_id_nickname_list()
    store_posts_comments_replies_for_profiles(profiles_id_list, profiles_nicknames_list)


if __name__ == "__main__":
    start_program()
