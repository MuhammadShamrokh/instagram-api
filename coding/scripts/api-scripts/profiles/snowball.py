from coding.APIConnectors.Data365Connector import Data365Connector
from coding.utility.InstagramAPIDatabaseHandler import InstagramAPIDatabaseHandler
from coding.loggers.Logger import logger
from datetime import datetime, timedelta
import pandas as pd

# ---------------- URLs ---------------------------
input_profiles_files_url = "../../../../data/api-data/profiles/NFL/seed-NFL-users-profile.csv"
user_profiles_db_url = "../../../../data/api-data/profiles/profiles.db"
# ---------------- Objects ------------------------
api_connector = Data365Connector()
database_connector = InstagramAPIDatabaseHandler(user_profiles_db_url)
# ---------------- CONSTS -------------------------
SNOWBALL_ITERATIONS = 1
MAX_AMOUNT = 100
THRESHOLD = 0
# calculating one week ago date
FROM_DATE = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
# ---------------- Data structure -----------------
# a set to store all the new profiles that were found during snowball (set to prevent duplicates)
new_profiles_set = set()

# array of dataframes to store data of the new profile found in each snowball iteration
profiles_dfs_lst = []


def init_dataframes_lst():
    for _ in range(SNOWBALL_ITERATIONS + 1):
        profiles_dfs_lst.append(pd.DataFrame(columns=['ID', 'Name', 'Nickname', 'Bio',
                                                      'Post_Count', 'Follower_Count', 'Following_Count',
                                                      'Is_Business', 'Is_Private', 'Is_Verified']))
    logger.info("Profiles dataframe list was initialized successfully! ")


def read_origin_profiles_df():
    origin_profiles_df = pd.read_csv(input_profiles_files_url)
    # adding the init dataframe to the dataframe list
    profiles_dfs_lst[0] = origin_profiles_df.iloc[THRESHOLD:]


def save_profile_with_engagement_in_db(profile_data, post_amount, engagement_count):
    profile_data_tuple = (profile_data['ID'], profile_data['Name'], profile_data['Nickname'], profile_data['Bio'],
                          profile_data['Post_Count'], profile_data['Follower_Count'], profile_data['Following_Count'],
                          profile_data['Is_Business'], profile_data['Is_Private'], profile_data['Is_Verified'],
                          post_amount, engagement_count)

    database_connector.save_profile_with_month_engagement_to_database("NFL_profiles_with_month_engagement",
                                                                      profile_data_tuple)


def extract_new_profile_data(idx, profile_id):

    profile_json = api_connector.get_profile_data_by_id(profile_id)
    if profile_json is not None:
        new_profile_data = {
            'ID': profile_json.get("id", None),
            'Name': profile_json.get("full_name", None),
            'Nickname': profile_json.get("username", None),
            'Bio': profile_json.get("biography", None),
            'Post_Count': profile_json.get("posts_count", -1),
            'Follower_Count': profile_json.get("followers_count", -1),
            'Following_Count': profile_json.get("followings_count", -1),
            'Is_Business': profile_json.get("is_business_account", "unknown"),
            'Is_Private': profile_json.get("is_private", "unknown"),
            'Is_Verified': profile_json.get("is_verified", "unknown")}
        # Convert the dictionary to a DataFrame
        new_profile_df = pd.DataFrame([new_profile_data])

        # Concatenate the original DataFrame and the new row DataFrame
        profiles_dfs_lst[idx] = pd.concat([profiles_dfs_lst[idx], new_profile_df], ignore_index=True)

        # saving profile (without engagement) to table
        database_connector.save_profile_to_database("NFL_Profiles", profile_json)


def calculate_profiles_engagement_store_in_db(profiles_df):
    for idx, profile in profiles_df.iterrows():
        profile_id = profile['ID']
        profile_engagement_during_last_month = 0

        logger.info("calculating profile with " + str(profile_id) + " id engagement. (" + str(idx) + "/" + str(
            len(profiles_df)+THRESHOLD) + ")")
        # fetching all profile posts from last month
        profile_posts_lst = api_connector.get_posts_by_profile_id(profile_id, MAX_AMOUNT, FROM_DATE)

        # scanning all posts to discover who interacted with same profile id
        for post in profile_posts_lst:
            # adding the post engagement to the profile total engagement in the last month
            if post['likes_count'] is not None:
                profile_engagement_during_last_month += post['likes_count']
            if post['comments_count'] is not None:
                profile_engagement_during_last_month += post['comments_count']

        save_profile_with_engagement_in_db(profile, len(profile_posts_lst), profile_engagement_during_last_month)


def find_store_new_profiles_from_comment(idx, comments_lst):
    new_profiles_found_from_post_comments_count = 0

    for comment in comments_lst:
        comment_owner_id = comment['owner_id']

        # checking if the profile id wasn't found before
        if comment_owner_id not in new_profiles_set:
            # saving new profile data to dataframe and file
            extract_new_profile_data(idx, comment_owner_id)
            # updating counters and data structures
            logger.debug("New profile with id " + str(comment_owner_id) + " was found during snowball process")
            new_profiles_found_from_post_comments_count += 1
            new_profiles_set.add(comment_owner_id)

    return new_profiles_found_from_post_comments_count


def find_profiles_from_posts_comments_calculate_profile_engagement(idx, profile, profile_posts_lst):
    # profile engagement is the number of likes and comments his posts has
    profile_engagement_during_period = 0

    # scanning all posts to discover who interacted with same profile id
    for j, post in enumerate(profile_posts_lst):
        post_id = post['id']

        # adding the post engagement to the profile total engagement in the last month
        if post['likes_count'] is not None:
            profile_engagement_during_period += post['likes_count']
        if post['comments_count'] is not None:
            profile_engagement_during_period += post['comments_count']

        logger.info(
            "Fetching post " + str(post_id) + " comment's which was posted by " + str(profile['ID']) + ". (" + str(
                j+1) + "/" + str(len(profile_posts_lst)) + ")")
        comments_lst = api_connector.get_post_comments(post_id, FROM_DATE, MAX_AMOUNT)

        # scanning comments to find new profiles that interacted with current profile
        new_profiles_found_from_post_comments_count = find_store_new_profiles_from_comment(idx, comments_lst)

        logger.info(
            str(new_profiles_found_from_post_comments_count) + " new profiles were found while scanning post " + str(
                post_id) + " comments")
    
    return profile_engagement_during_period


def snowball(profiles_df, idx=0):
    # scanning all profiles to snowball
    for i, profile in profiles_df.iterrows():
        logger.info("Snowballing profile with " + str(profile['ID']) + " id. (" + str(i+1) + "/" + str(len(profiles_df)+THRESHOLD) + ")")

        # fetching all profile posts from last month
        profile_posts_lst = api_connector.get_posts_by_profile_id(profile['ID'], MAX_AMOUNT, FROM_DATE)
        logger.info("Profile " + str(profile['ID']) + " has posted " + str(len(profile_posts_lst)) + " posts in the given period")

        # scanning profile posts to calculate engagement and find new profiles
        profile_engagement_during_period = find_profiles_from_posts_comments_calculate_profile_engagement(idx, profile, profile_posts_lst)

        # saving profile with engagement data in a database
        save_profile_with_engagement_in_db(profile, len(profile_posts_lst), profile_engagement_during_period)


def main():
    # init dataframes list
    init_dataframes_lst()
    # read initial profiles
    read_origin_profiles_df()

    # snowballing 'SNOWBALL_ITERATIONS' times to find more profiles
    for idx in range(SNOWBALL_ITERATIONS):
        logger.info("Snowball iteration " + str(idx + 1) + " has began ...")
        # snowballing to get new list of profiles id
        snowball(profiles_dfs_lst[idx], idx + 1)
        
    # calculating the engagement of profiles that were found in the last iteration (we didn't snowball to discover engagement)
    calculate_profiles_engagement_store_in_db(profiles_dfs_lst[SNOWBALL_ITERATIONS])


if __name__ == "__main__":
    main()
