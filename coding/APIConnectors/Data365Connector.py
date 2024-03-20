from datetime import datetime, timedelta
from coding.loggers.Logger import logger
import requests
import json
import time


class Data365Connector:
    api_access_token = "ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKSVV6STFOaUo5LmV5SnpkV0lpT2lKV2FXVnlZV3hzZVNJc0ltbGhkQ0k2TVRjd05UTXpNVEF4Tmk0NU1UY3lNVGcwZlEuZk1RMUFjS3FfSF9FWWNxY1M5aFZVNm1UbmMtWGlrWm9uZFdRcXpKOFZ3TQ=="

    # api profile's URL
    profile_task_base_url = 'https://api.data365.co/v1.1/instagram/profile/'
    hashtag_search_update_base_url = "https://api.data365.co/v1.1/instagram/tag/"
    hashtag_search_fetch_data_base_url = 'https://api.data365.co/v1.1/instagram/tag/{tag_id}'
    # api post's URL
    post_task_base_url = 'https://api.data365.co/v1.1/instagram/post/'

    def get_profile_data_by_id(self, profile_id):
        profile_data = None

        # sending update task
        update_url = self.profile_task_base_url + str(profile_id) + "/update"
        update_query_params = {"access_token": Data365Connector.api_access_token}
        update_task_response = self._create_update_request(update_url, update_query_params)

        if update_task_response.status_code == 202:
            # waiting till the update task ends
            is_update_task_done_with_success = self._wait_for_update_task_to_finish(update_url, update_query_params)

            if is_update_task_done_with_success:
                # fetching profile data
                profile_data = self.get_cached_profile_data(profile_id)

            else:
                logger.warning("Data365Connector: Caching profile "+str(profile_id)+" data into Data365 API databases process failed!")
        else:
            logger.warning("Data365Connector: Could not start profile "+str(profile_id)+" update task")

        return profile_data

    def get_cached_profile_data(self, profile_id):
        profile_data = None

        try:
            fetch_data_url = self.profile_task_base_url + str(profile_id)
            fetch_query_params = {"access_token": Data365Connector.api_access_token}
            # sending GET request to extract the data from the databases
            data_fetch_response = self._get_stored_data(fetch_data_url, fetch_query_params)
            response_dict = json.loads(data_fetch_response.text)

            if data_fetch_response.status_code == 200:
                profile_data = response_dict['data']
            else:
                logger.warning(
                    "Data365Connector: GET profile " + str(profile_id) + " data request has failed")
        except Exception:
            logger.warning("Data365Connector: Something went wrong while fetching profile "+str(profile_id)+" data!")

        return profile_data

    def get_post_by_id(self, post_id):
        pass

    def get_comment_by_id(self, comment_id):
        pass

    def get_posts_by_hashtag(self, hashtag, max_posts, from_date, to_date=None, num_of_pages=5):
        posts_lst = list()

        # sending update task
        logger.debug("Data365Connector: Sending Post request to start a search posts by hashtag "+hashtag+" update task")
        update_url = self.hashtag_search_update_base_url + hashtag + "/update"
        update_query_params = {'from_date': from_date, 'max_posts': max_posts, 'load_posts_data': True, 'access_token': Data365Connector.api_access_token}
        update_task_response = self._create_update_request(update_url, update_query_params)

        if update_task_response.status_code == 202:
            # waiting till the update task ends
            logger.debug("Data365Connector: waiting for search posts by hashtag "+hashtag+" update task to finish")
            token_query_params = {'access_token': Data365Connector.api_access_token}
            is_update_task_done_with_success = self._wait_for_update_task_to_finish(update_url, token_query_params)

            if is_update_task_done_with_success:
                logger.info('Data365Connector: the amount of received posts that include '+hashtag+" hashtag is "+str(self._get_amount_of_posts_for_hashtag(hashtag)))
                # fetching all posts that were found in hashtag search
                get_posts_url = self.hashtag_search_update_base_url + hashtag + '/posts'
                get_post_query_params = {'from_date': from_date, 'to_date': to_date, 'lang': 'en', "max_page_size": 100,
                                         'access_token': self.api_access_token}

                posts_lst = self._fetch_data_return_list('posts', get_posts_url, get_post_query_params, number_of_pages=num_of_pages)
            else:
                logger.warning("Data365Connector: Caching posts that include hashtag "+hashtag+" into Data365 API databases process failed!")

        else:
            logger.warning("Data365Connector: Could not start search posts by hashtag "+hashtag+" update task")

        return posts_lst

    def get_cached_hashtag_posts(self, hashtag, from_date, to_date=None, num_of_pages=5):
        # preparing url and query params
        get_posts_url = self.hashtag_search_update_base_url + hashtag + '/posts'
        get_post_query_params = {'from_date': from_date, 'to_date': to_date, 'lang': 'en', "max_page_size": 100,
                                 'access_token': self.api_access_token}

        posts_lst = self._fetch_data_return_list('posts', get_posts_url, get_post_query_params, number_of_pages=num_of_pages)

        return posts_lst

    def get_posts_by_profile_id(self, profile_id, max_posts=10, from_date=None, to_date=None):
        # list to insert profile posts into
        posts_list = list()

        # url and query params to init an update task
        update_url = self.profile_task_base_url + str(profile_id) + "/update"
        # query params for post request
        update_query_params = {"from_date": from_date, "load_feed_posts": True, "max_posts": max_posts,
                               "access_token": self.api_access_token}
        # query params for status and get request
        access_token_query_params = {"access_token": self.api_access_token}

        update_response = requests.post(update_url, params=update_query_params)
        # checking post request status code
        if update_response.status_code == 202:
            logger.debug("Data365Connector: Post request was sent successfully - response status code is 202.")
            data_cached = self._wait_for_update_task_to_finish(update_url, access_token_query_params)
            # data was cached in API
            if data_cached:
                get_posts_data_url = self.profile_task_base_url + str(profile_id) + "/feed/posts"
                get_request_query_parameters = {"from_date": from_date, "to_date": to_date, "order_by": "date_desc", "max_page_size": 100,
                                                "access_token": self.api_access_token}

                posts_list = self._fetch_data_return_list("Posts", get_posts_data_url, get_request_query_parameters)
        else:
            logger.warning("Data365Connector: POST request to start update task for profile "+str(profile_id)+" has failed")

        return posts_list

    def get_cached_profile_posts(self, profile_id, from_date):
        get_posts_data_url = self.profile_task_base_url + str(profile_id) + "/feed/posts"
        get_request_query_parameters = {"from_date": from_date, "order_by": "date_desc", "max_page_size": 100,
                                        "access_token": self.api_access_token}

        posts_list = self._fetch_data_return_list("Posts", get_posts_data_url, get_request_query_parameters)

        return posts_list

    def get_post_comments(self, post_id, from_date, max_comments):
        # list to insert post comments into
        comments_list = list()

        # url and query params to init an update task
        update_url = self.post_task_base_url + str(post_id) + "/update"
        # query params for post request
        update_query_params = {"from_date": from_date, "load_comments": True, "max_posts": max_comments,
                               "access_token": self.api_access_token}
        # query params for status and get request
        access_token_query_params = {"access_token": self.api_access_token}

        update_response = requests.post(update_url, params=update_query_params)
        # checking post request status code
        if update_response.status_code == 202:
            data_cached = self._wait_for_update_task_to_finish(update_url, access_token_query_params)
            # data was cached in API
            if data_cached:
                get_comments_data_url = self.post_task_base_url + str(post_id) + "/comments"
                get_request_query_parameters = {"from_date": from_date, "order_by": "date_desc", "max_page_size": 100,
                                                "access_token": self.api_access_token}

                comments_list = self._fetch_data_return_list("comments", get_comments_data_url, get_request_query_parameters)
        else:
            logger.warning("Data365Connector: POST request to update post "+str(post_id)+" comment's has failed")

        return comments_list

    def get_comment_replies(self, comment_id):
        pass

    def get_profile_data_tuple_from_json(self, profile_json):

        profile_id = profile_json.get("id", None)

        name = profile_json.get("full_name", None)
        if name is None:
            name = "unknown"

        nickname = profile_json.get("username", None)
        if nickname is None:
            nickname = "unknown"

        bio = profile_json.get("biography", None)
        if bio is None:
            bio = "No biography"

        post_count = profile_json.get("posts_count", -1)
        if post_count is None:
            post_count = -1

        followers_count = profile_json.get("followers_count", -1)
        if followers_count is None:
            followers_count = -1

        following_count = profile_json.get("followings_count", -1)
        if following_count is None:
            following_count = -1

        business = profile_json.get("is_business_account", "unknown")
        if business is None:
            business = "unknown"

        private = profile_json.get("is_private", "unknown")
        if private is None:
            private = "unknown"

        verified = profile_json.get("is_verified", "unknown")
        if verified is None:
            verified = "unknown"

        return profile_id, name, nickname, bio, post_count, followers_count, following_count, business, private, verified

    def get_post_data_tuple_from_json(self, post_json):
        post_id = post_json.get("id", None)

        caption = post_json.get("text", None)
        if caption is None:
            caption = "no caption"

        owner_id = post_json.get("owner_id", None)
        if owner_id is None:
            owner_id = "unknown"

        owner_username = post_json.get("owner_username", None)
        if owner_username is None:
            owner_username = "unknown"

        likes_count = post_json.get("likes_count", -1)
        if likes_count is None:
            likes_count = -1

        comments_count = post_json.get("comments_count", -1)
        if comments_count is None:
            comments_count = -1

        publication_timestamp = post_json.get("timestamp", None)
        if publication_timestamp is None:
            publication_timestamp = "unknown"
            publication_date = "unknown"
        else:
            publication_date = datetime.fromtimestamp(publication_timestamp).date()

        location_id = post_json.get("location_id", None)
        if location_id is None:
            location_id = "unknown"

        return post_id, caption, owner_id, owner_username, likes_count, comments_count, publication_date, publication_timestamp, location_id

    def _fetch_data_return_list(self, data_type, url, query_params, number_of_pages=0):
        data_list = list()
        pages_count = 0

        try:
            # scanning all the data pages
            while True:
                # sending request to get data
                get_data_response = requests.get(url, params=query_params)

                # reading the response body (turning json into dictionary to work with
                data_response_body_dict = json.loads(get_data_response.text)

                if get_data_response.status_code == 200:
                    # adding new data from last GET request to data list
                    data_list.extend(list(data_response_body_dict["data"]["items"]))
                    # extracting cursor
                    page_info = data_response_body_dict["data"]["page_info"]
                    pages_count = pages_count + 1
                    logger.debug("The " + str(pages_count) + " page of " + data_type + " information was successfully read")
                    if page_info["has_next_page"] and (pages_count < number_of_pages or number_of_pages == 0):
                        logger.debug(
                            "Theres another page of " + data_type + " information, going to send another get request.")
                        # there is another page of posts data, we get the cursor to use in the next get request
                        query_params['cursor'] = page_info["cursor"]

                    else:
                        logger.debug("This was the last page of " + data_type + " information.")
                        break
                else:
                    logger.warning("Data365Connector: GET request to fetch "+data_type+" data has failed")

                time.sleep(0.1)
        except Exception as e:
            logger.warning("Data365Connector: Something went wrong while fetching "+data_type+" data list")

        return data_list

    def _get_amount_of_posts_for_hashtag(self, hashtag):
        amount_of_posts = 0

        # sending data request
        url = self.hashtag_search_update_base_url + hashtag
        token_query_param = {'access_token': Data365Connector.api_access_token}
        response = self._get_stored_data(url, token_query_param)

        if response.status_code == 200:
            if response is not None:
                response_content_dict = json.loads(response.text)
                amount_of_posts = response_content_dict['data']['posts_count']

        return amount_of_posts

    def _create_update_request(self, url, query_params):
        response = requests.post(url, params=query_params)

        return response

    def _wait_for_update_task_to_finish(self, url, query):
        data_cached = False

        try:
            while True:
                status_response = requests.get(url, params=query)
                # checking GET status request status code
                if status_response.status_code == 200:
                    logger.debug("Data365Connector: GET status request was sent successfully - response status code is 200.")
                    status_response_body = json.loads(status_response.text)

                    if status_response_body['data']['status'] == 'finished':
                        logger.debug("Data365Connector: The status of the profile update request is 'finished'.")
                        data_cached = True
                        break
                    elif status_response_body['data']['status'] == 'fail' or status_response_body['data']['status'] == 'canceled':
                        logger.warning("Data365Connector: Post request status is " + status_response_body['data']['status'] + ".")
                        break
                    else:
                        # the profile search update task is not finished yet
                        logger.debug("Data365Connector: the profile  update task is not finished yet, current status is: " +
                                     status_response_body['data']['status'])
                        time.sleep(5)
                else:  # GET status request failed
                    logger.warning("Data365Connector: GET request to check the update process status has failed")
                    break
        except Exception as e:
            data_cached = False
            logger.warning("Data365Connector: Something went wrong while waiting for a task to finish....")

        return data_cached

    def _get_stored_data(self, url, query):
        # sending GET request to extract the data from the databases
        get_data_response = requests.get(url, params=query)

        return get_data_response
