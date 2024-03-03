import os
import requests
from urllib.parse import urlparse
import json
import logging
import time

input_file_url = "../../../api-data/random-account/posts.csv"
images_folder_url = "../../api-data/random-images"

images_download_counter = 0
# --------------- Logger objects ---------------------

formatter = logging.Formatter(fmt="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
                              datefmt='%d-%m-%Y %H:%M:%S')
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)
screen_handler = logging.StreamHandler()
screen_handler.setFormatter(formatter)
logger.addHandler(screen_handler)

# ------------------  API URLS  --------------------
api_access_token = "ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKSVV6STFOaUo5LmV5SnpkV0lpT2lKV2FXVnlZV3hzZVNJc0ltbGhkQ0k2TVRjd05UTXpNVEF4Tmk0NU1UY3lNVGcwZlEuZk1RMUFjS3FfSF9FWWNxY1M5aFZVNm1UbmMtWGlrWm9uZFdRcXpKOFZ3TQ=="

post_request_base_url = "https://api.data365.co/v1.1/instagram/post/"
get_data_base_url = "https://api.data365.co/v1.1/instagram/post/"
post_query_parameters = {"load_comments": False, "access_token": api_access_token}
query_parameters = {"access_token": api_access_token}


def download_and_save_image(image_url, local_folder):
    global images_download_counter

    try:
        # Make a GET request to the image URL
        response = requests.get(image_url)
        response.raise_for_status()  # Raise an exception for bad responses

        # Extract the image file name from the URL and sanitize it
        parsed_url = urlparse(image_url)
        image_name = os.path.basename(parsed_url.path)

        # Remove invalid characters from the image name
        image_name = ''.join(c for c in image_name if c.isalnum() or c in ('.', '_'))

        # Ensure the local folder exists, create it if not
        os.makedirs(local_folder, exist_ok=True)

        # Construct the local file path
        local_file_path = os.path.join(local_folder, image_name)

        # Open the local file in binary write mode and write the content
        with open(local_file_path, 'wb') as local_file:
            local_file.write(response.content)

        logger.debug(f"Image downloaded and saved to '{local_file_path}' successfully.")
        images_download_counter = images_download_counter + 1
        logger.info("Done downloading " + str(images_download_counter) + " posts images to local folder ...")

    except requests.exceptions.RequestException as e:
        logger.warning(f"Error downloading image from {image_url}: {e}")
    except Exception as e:
        logger.warning(f"Error saving image: {e}")


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
                    time.sleep(1)
            else:  # GET status request failed
                status_response_body_dict = json.loads(status_response.text)
                logger.warning("GET status request has failed, error: " + str(status_response_body_dict["error"]))
                break
    except Exception as e:
        data_cached = False
        logger.warning("Something went wrong.... (line 115)")

    return data_cached


def read_posts_file():
    with open(input_file_url, "r") as posts_file:
        posts_json_list = posts_file.read().splitlines()

    posts_json_list = [json.loads(json_str) for json_str in posts_json_list]
    return_list = posts_json_list[3000:]

    return return_list


def fetch_posts_data_download_images(posts_list):

    for post in posts_list:
        post_id = post["id"]

        logger.info("Fetching Post "+str(post_id)+" images...")
        # sending POST request to get instagram post data
        start_update_task_url = post_request_base_url + str(post_id) + "/update"
        update_response = requests.post(start_update_task_url, params=post_query_parameters)
        # checking POST request status code
        if update_response.status_code == 202:
            logger.debug("Post request was sent successfully - response status code is 202.")
            # waiting for UPDATE task to finish
            data_cached = wait_for_database_update_task_to_finish(start_update_task_url, query_parameters)
            # data was cached in API
            if data_cached:
                # fetching instagram post updated data
                get_data_url = get_data_base_url + str(post_id)
                get_data_response = requests.get(get_data_url, params=query_parameters)
                data_response_body_dict = json.loads(get_data_response.text)
                post_data = data_response_body_dict['data']

                if post_data['attached_carousel_media_urls'] is not None:
                    media_url_list = post_data['attached_carousel_media_urls']
                    for media_url in media_url_list:
                        download_and_save_image(media_url, images_folder_url)

                elif post_data['attached_media_display_url'] is not None:
                    download_and_save_image(post_data['attached_media_display_url'], images_folder_url)


def start_program():
    posts_list = read_posts_file()
    fetch_posts_data_download_images(posts_list)


if __name__ == "__main__":
    start_program()
