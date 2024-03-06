import requests
import json
from loggers.Logger import logger
import time


class Data365Connector:
    api_access_token = "ZXlKMGVYQWlPaUpLVjFRaUxDSmhiR2NpT2lKSVV6STFOaUo5LmV5SnpkV0lpT2lKV2FXVnlZV3hzZVNJc0ltbGhkQ0k2TVRjd05UTXpNVEF4Tmk0NU1UY3lNVGcwZlEuZk1RMUFjS3FfSF9FWWNxY1M5aFZVNm1UbmMtWGlrWm9uZFdRcXpKOFZ3TQ=="

    # api profile's URL
    profile_task_base_url = 'https://api.data365.co/v1.1/instagram/profile/'
    hashtag_search_update_base_url = "https://api.data365.co/v1.1/instagram/tag/{tag_id}/update"
    hashtag_search_fetch_data_base_url = 'https://api.data365.co/v1.1/instagram/tag/{tag_id}'
    # api post's URL
    post_update_base_url = 'https://api.data365.co/v1.1/instagram/post/{post_id}/update'
    comment_data_fetch_base_url = 'https://api.data365.co/v1.1/instagram/comment/{comment_id}'
    comments_replies_fetch_base_url = 'https://api.data365.co/v1.1/instagram/comment/{comment_id}/replies'

    def get_profile_data_by_id(self, profile_id, cached_data=False):
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
                logger.warning("Data365Connector: Caching profile "+str(profile_id)+" data into databases process failed!")
        else:
            logger.warning("Data365Connector: Could not start profile "+str(profile_id)+" update task, error: "+str(json.loads(update_task_response.text)["error"]))

        return profile_data

    def get_cached_profile_data(self, profile_id):
        profile_data = None
        fetch_data_url = self.profile_task_base_url + str(profile_id)
        fetch_query_params = {"access_token": Data365Connector.api_access_token}
        # sending GET request to extract the data from the databases
        data_fetch_response = self._get_stored_data(fetch_data_url, fetch_query_params)
        response_dict = json.loads(data_fetch_response.text)

        if data_fetch_response.status_code == 200:
            profile_data = response_dict['data']
        else:
            logger.warning(
                "Data365Connector: GET profile " + str(profile_id) + " data request has failed, error: " + str(
                    response_dict["error"]))

        return profile_data

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

    def get_post_by_id(self, post_id):
        pass

    def get_comment_by_id(self, comment_id):
        pass

    def search_posts_by_hashtag(self, hashtag):
        pass

    def get_profile_posts(self, profile_id, max_posts, from_date):
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
            logger.debug("Post request was sent successfully - response status code is 202.")
            data_cached = self._wait_for_update_task_to_finish(update_url, access_token_query_params)
            # data was cached in API
            if data_cached:
                get_posts_data_url = self.profile_task_base_url + str(profile_id) + "/feed/posts"

                posts_list = self._fetch_data_return_list("Posts", get_posts_data_url, from_date)
        else:
            update_response_body_dict = json.loads(update_response.text)
            logger.warning("POST request has failed, error: " + str(update_response_body_dict["error"]))

        return posts_list

    def get_cached_profile_posts(self, profile_id, max_posts, from_date):
        get_posts_data_url = self.profile_task_base_url + str(profile_id) + "/feed/posts"
        posts_list = self._fetch_data_return_list("Posts", get_posts_data_url, from_date)

        return posts_list

    def get_post_comments(self, post_id, from_date, max_comments):
        # list to insert post comments into
        comments_list = list()

        # url and query params to init an update task
        update_url = self.post_update_base_url + str(post_id) + "/update"
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
                get_comments_data_url = self.post_update_base_url + str(post_id) + "/comments"

                posts_list = self._fetch_data_return_list("comments", get_comments_data_url, from_date)
        else:
            update_response_body_dict = json.loads(update_response.text)
            logger.warning("POST request has failed, error: " + str(update_response_body_dict["error"]))

        return comments_list

    def get_comment_replies(self, comment_id):
        pass

    def _fetch_data_return_list(self, data_type, url, from_date):
        cursor = None
        data_list = list()
        pages_count = 0

        # query params to send a GET request
        get_request_query_parameters = {"from_date": from_date, "order_by": "date_desc", "max_page_size": 100,
                                        "access_token": self.api_access_token}
        # query params to send a *next* GET request
        get_request_with_cursor_query_parameters = {"from_date": from_date, "order_by": "date_desc",
                                                    "max_page_size": 100, "cursor": "",
                                                    "access_token": self.api_access_token}

        try:
            # scanning all the data pages
            while True:
                # first page of data
                if cursor is None:
                    get_data_response = requests.get(url, params=get_request_query_parameters)
                else:
                    get_request_with_cursor_query_parameters["cursor"] = cursor
                    get_data_response = requests.get(url, params=get_request_with_cursor_query_parameters)

                # reading the response body (turning json into dictionary to work with
                data_response_body_dict = json.loads(get_data_response.text)

                if get_data_response.status_code == 200:
                    # adding new data from last GET request to data list
                    data_list.extend(list(data_response_body_dict["data"]["items"]))
                    # extracting cursor
                    page_info = data_response_body_dict["data"]["page_info"]
                    pages_count = pages_count + 1
                    logger.debug("The " + str(pages_count) + " page of " + data_type + " information was saved")
                    if page_info["has_next_page"]:
                        logger.debug(
                            "Theres another page of " + data_type + " information, going to send another get request.")
                        # there is another page of posts data, we get the cursor to use in the next get request
                        cursor = page_info["cursor"]
                    else:
                        logger.debug("This was the last page of " + data_type + " information.")
                        break
                else:
                    logger.warning("GET data request has failed, error: " + str(data_response_body_dict["error"]))

                time.sleep(0.1)
        except Exception as e:
            logger.warning("Something went wrong. (line 161)")

        return data_list

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
                    status_response_body_dict = json.loads(status_response.text)
                    logger.warning("Data365Connector: GET status request has failed, error: " + str(status_response_body_dict["error"]))
                    break
        except Exception as e:
            data_cached = False
            logger.warning("Data365Connector: Something went wrong while waiting for a task to finish....")

        return data_cached

    def _get_stored_data(self, url, query):
        # sending GET request to extract the data from the databases
        get_data_response = requests.get(url, params=query)

        return get_data_response
