from coding.APIConnectors.Data365Connector import Data365Connector
from coding.utility.InstagramAPIDatabaseHandler import InstagramAPIDatabaseHandler
from coding.loggers.Logger import logger
import pandas as pd
from io import BytesIO
from PIL import Image
import requests
import os

# ---------------- URLs ---------------------------
posts_db_url = "../../../../data/api-data/posts-comments-replies/posts.db"
images_folder_url = "../../../../data/api-data/posts-comments-replies/test-set/media"
profiles_id_input_file = "../../../../data/api-data/posts-comments-replies/test-set/input.parquet"
# ---------------- Objects ------------------------
api_connector = Data365Connector()
database_connector = InstagramAPIDatabaseHandler(posts_db_url)


def get_posts_id_list():
    """
    the function reads input parquet file that include posts id's
    then returns a list of posts id
    """
    posts_df = pd.read_parquet(profiles_id_input_file)

    return list(posts_df["ID"])[:5]


def download_and_save_post_media(post_json):
    media_count = 0
    post_id = post_json["id"]

    if post_json["attached_carousel_media_urls"] is not None:
        # extracting media urls from post data json
        media_urls = post_json["attached_carousel_media_urls"]
        # scanning all urls to download the media
        for url in media_urls:
            file_name = str(post_id) + "_" + str(media_count)
            download_media(url, filename=file_name)
            media_count = media_count + 1

        logger.info("Downloaded " + str(media_count) + " image/video using " + str(post_id) + " post id.")
        # attached_carousel_media_urls includes the media that appears in attached_media_display_url and attached_video_url
        return

    if post_json["attached_media_display_url"] is not None:
        media_url = post_json["attached_media_display_url"]
        file_name = str(post_id) + "_" + str(media_count)
        download_media(media_url, file_name)
        media_count = media_count + 1

    if post_json["attached_video_url"] is not None:
        media_url = post_json["attached_video_url"]
        file_name = str(post_id) + "_" + str(media_count)
        download_media(media_url, file_name)

    logger.info("Downloaded "+str(media_count)+" image/video using "+str(post_id)+" post id.")


def download_media(url, filename, folder=images_folder_url):
    """
    Download a media file (image or video) from a URL and save it to a specified folder.

    Args:
    - url (str): URL of the media file.
    - folder (str): Folder to save the media file to. Default is the current directory.
    - filename (str): Name of the file to save the media file to (without extension).

    Returns:
    - success (bool): True if the media file was downloaded and saved successfully, False otherwise.
    """
    try:
        # Send a HEAD request to get the media type
        response = requests.head(url)

        # Check if the request was successful
        if response.status_code == 200:
            # Get the content type from the response headers
            content_type = response.headers.get('content-type')

            # Determine the file extension based on the content type
            if 'image' in content_type:
                file_extension = '.jpg'
            elif 'video' in content_type:
                file_extension = '.mp4'
            else:
                logger.debug("Unsupported media type")
                return False

            # Create the specified folder if it doesn't exist
            if not os.path.exists(folder):
                os.makedirs(folder)

            # Download and save the media file
            filepath = os.path.join(folder, f"{filename}{file_extension}")
            with open(filepath, 'wb') as f:
                media_response = requests.get(url)
                f.write(media_response.content)

            logger.debug(f"Media file downloaded and saved as {filepath}")
            return True
        else:
            logger.warning(f"Failed to download media file: HTTP status code {response.status_code}")
            return False
    except Exception as e:
        logger.error("An error occurred: {e}")
        return False


def main():
    logger.info("Extracting posts by id process has began...")
    # reading posts id that we want to extract their data
    posts_id_list = get_posts_id_list()
    logger.info(str(len(posts_id_list)) + " Posts id's were received as an input...")

    # scanning posts id
    for idx, post_id in enumerate(posts_id_list):
        logger.info("Extracting "+str(post_id)+" Post data using instagram API... ("+str(idx+1)+"/"+str(len(posts_id_list))+")")
        # extracting post data json from instagram api
        post_data_json = api_connector.get_post_by_id(post_id)
        # saving post data into database
        database_connector.save_post_to_database("Posts_Test_Set", api_connector.get_post_data_tuple_from_json(post_data_json))
        # saving post media into media folder
        download_and_save_post_media(post_data_json)


if __name__ == "__main__":
    # main()
    database_connector.delete_database_table("Posts_Test_Set")
