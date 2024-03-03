import os
import requests
from urllib.parse import urlparse
import json
import logging


input_file_url = "../../api-data/general-insurance/general-insurance-posts.csv"
images_folder_url = "../../api-data/general-insurance/images"

# --------------- Logger objects ---------------------

formatter = logging.Formatter(fmt="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
                              datefmt='%d-%m-%Y %H:%M:%S')
logger = logging.getLogger("logger")
logger.setLevel(logging.INFO)
screen_handler = logging.StreamHandler()
screen_handler.setFormatter(formatter)
logger.addHandler(screen_handler)

image_download_counter = 0


def download_and_save_image(image_url, local_folder):
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

    except requests.exceptions.RequestException as e:
        logger.warning(f"Error downloading image from {image_url}: {e}")
    except Exception as e:
        logger.warning(f"Error saving image: {e}")


def print_image_download_counter():
    global image_download_counter

    image_download_counter = image_download_counter + 1
    logger.debug("Done downloading " + str(image_download_counter) + " Images....")


def start_program():
    global image_download_counter

    # Open the file in read mode
    logger.info("Posts images stored in "+input_file_url+" downloading process has began.")
    with open(input_file_url, 'r') as file:
        # Read each line and interpret it as JSON
        for line in file:
            try:
                json_data = json.loads(line)
                # multiple images
                if json_data['attached_carousel_media_urls'] is not None:
                    logger.info("Downloading " + str(json_data["id"]) + " Post Multiple images")
                    media_url_list = json_data['attached_carousel_media_urls']
                    for media_url in media_url_list:
                        download_and_save_image(media_url, images_folder_url)
                        print_image_download_counter()

                # one image only
                elif json_data['attached_media_display_url'] is not None:
                    logger.info("Downloading " + str(json_data["id"]) + " Post image")
                    download_and_save_image(json_data['attached_media_display_url'], images_folder_url)

                    print_image_download_counter()

            except json.JSONDecodeError as e:
                logger.warning(f"Error decoding JSON on line: {e}")

    logger.info("Done downloading " + str(image_download_counter) + " images from the file: " + input_file_url)


if __name__ == "__main__":
    start_program()
