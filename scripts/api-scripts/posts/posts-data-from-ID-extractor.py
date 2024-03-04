import json
import logging
import pandas as pd
import requests
import time
import sys
import os
import datetime

posts_id_input_file_url = "../../../api-data/posts-by-topics.csv"
posts_data_output_file_url = "../../../api-data/general-insurance/general-insurance-posts.csv"

posts_update_task_base_url = "https://api.data365.co/v1.1/instagram/post/"
get_post_data_base_url = "https://api.data365.co/v1.1/instagram/post/"

api_access_token = "ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKSVV6STFOaUo5LmV5SnpkV0lpT2lKV2FXVnlZV3hzZVNJc0ltbGhkQ0k2TVRjd05UTXpNVEF4Tmk0NU1UY3lNVGcwZlEuZk1RMUFjS3FfSF9FWWNxY1M5aFZVNm1UbmMtWGlrWm9uZFdRcXpKOFZ3TQ=="

query_parameters = {"access_token": api_access_token}

# --------------- Logger objects ---------------------
formatter = logging.Formatter(fmt="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
                              datefmt='%d-%m-%Y %H:%M:%S')
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)
screen_handler = logging.StreamHandler()
screen_handler.setFormatter(formatter)
logger.addHandler(screen_handler)


def get_posts_id_list_from_file(i_column_name):
    try:
        posts_url_df = pd.read_csv(posts_id_input_file_url)
        posts_url_by_category_list = list(posts_url_df[i_column_name])
        ret_posts_url_list = posts_url_by_category_list[0:]
    except Exception as err:
        print(err)
        sys.exit(0)

    return ret_posts_url_list


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


def store_posts_data_in_file(i_posts_id_list):
    logger.info("Fetching posts data process has began !...")
    for post_id in i_posts_id_list:
        # Setting timer for some API Reliability checking
        start = datetime.datetime.now()
        logger.info("Getting "+post_id+" post data")
        # preparing update url to start a task
        update_url = posts_update_task_base_url+str(post_id)+"/update"
        update_response = requests.post(update_url, params=query_parameters)

        if update_response.status_code == 202:
            logger.debug("Post request was sent successfully - response status code is 202.")
            data_cached = wait_for_database_update_task_to_finish(update_url, query_parameters)
            # data was cached in API databases
            if data_cached:
                # preparing url to get post data
                get_post_data_url = get_post_data_base_url + str(post_id)
                # sending GET request to retrieve post data
                get_data_response = requests.get(get_post_data_url, params=query_parameters)
                data_response_body_dict = json.loads(get_data_response.text)

                if get_data_response.status_code == 200:
                    logger.debug("GET post data request was sent successfully - response status code is 200.")

                    with open(posts_data_output_file_url, 'a') as file:
                        post_data_json = data_response_body_dict["data"]
                        post_data_str = json.dumps(post_data_json)
                        file.write(post_data_str + '\n')
                else:
                    logger.warning("GET data request has failed, error: " + str(data_response_body_dict["error"]))
        else:
            update_response_body_dict = json.loads(update_response.text)
            logger.warning("POST request has failed, error: " + str(update_response_body_dict["error"]))

        end = datetime.datetime.now()
        duration = int((end - start).total_seconds())
        logger.info("Done Fetching and saving " + str(post_id) + " post data. duration: " + str(duration) + " seconds.")

    logger.info("Fetching posts data process ended !...")


def main():
    posts_id_list = get_posts_id_list_from_file('Insurance General')
    store_posts_data_in_file(posts_id_list)


if __name__ == '__main__':
    main()
