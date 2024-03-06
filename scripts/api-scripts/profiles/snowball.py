from utility.InstagramAPIDatabaseHandler import InstagramAPIDatabaseHandler
from APIConnectors.Data365Connector import Data365Connector
from datetime import datetime, timedelta
from loggers.Logger import logger
import pandas as pd

# ---------------- URLs ---------------------------
input_profiles_files_url = "../../../api-data/profiles/NFL/NFL-users-profile.csv"
output_profiles_file_url = "../../../api-data/profiles/NFL/snowball-NFL-profiles.csv"
user_profiles_db_url = "../../../api-data/profiles/profiles.db"
# ---------------- Objects ------------------------
api_connector = Data365Connector()
database_connector = InstagramAPIDatabaseHandler(user_profiles_db_url)
# ---------------- CONSTS -------------------------
SNOWBALL_ITERATIONS = 2
MAX_AMOUNT = 1000
# ---------------- Data structure -----------------
# a set to store all the new profiles that were found during snowball (set to prevent duplicates)
new_profiles_set = set()
# a dictionary to store amount of posts in last month for each profile
profiles_amount_of_posts_last_month_dict = dict()
profile_engagement_last_month_dict = dict()
# array of dataframes to store data of the new profile found in each snowball iteration
profiles_dfs_lst = []


def read_origin_profiles_df():
    origin_profiles_df = pd.read_csv(input_profiles_files_url)
    # origin profiles, snowball iteration is 0
    origin_profiles_df['Snowball_Iteration'] = 0
    # adding the init dataframe to the dataframe list
    profiles_dfs_lst.append(origin_profiles_df)
    

def snowball(profiles_id_lst):
    current_snowball_new_profiles_id_list = list()

    # scanning all profiles to snowball
    for idx, profile_id in enumerate(profiles_id_lst):
        # profile engagement is the number of likes and comments his posts has 
        profile_engagement_during_last_month = 0
        
        logger.info("Snowballing profile with " + str(profile_id) + " id. ("+str(idx)+"/("+str(len(profiles_id_lst))+")")
        # fetching all profile posts from last month
        one_month_ago_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        profile_posts_lst = api_connector.get_profile_posts(profile_id, MAX_AMOUNT, one_month_ago_date)
        
        # scanning all posts to discover who interacted with same profile id
        for post in profile_posts_lst:
            post_id = post['id']
            # adding the post engagement to the profile total engagement in the last month
            profile_engagement_during_last_month += post['likes_count']
            profile_engagement_during_last_month += post['comments_count']
            
            logger.debug("Fetching data for post " + str(post_id) + " who was posted by " + str(profile_id))
            comments_lst = api_connector.get_post_comments(post_id, one_month_ago_date, MAX_AMOUNT)

            # scanning comments to store owner id of each comment
            for comment in comments_lst:
                comment_owner_id = comment['owner_id']
                # checking if the profile id wasn't found before
                if comment_owner_id not in new_profiles_set:
                    logger.debug("New profile with id "+str(comment_owner_id)+" was found during snowball process")
                    current_snowball_new_profiles_id_list.append(comment_owner_id)
                    new_profiles_set.add(comment_owner_id)
        
        # storing profile engagement and amount of posts in relevant dicts
        profiles_amount_of_posts_last_month_dict[profile_id] = len(profile_posts_lst)
        profile_engagement_last_month_dict[profile_id] = profile_engagement_during_last_month

    return current_snowball_new_profiles_id_list


def extract_new_profiles_data(profiles_id_lst, snowball_iteration):
    # init an empty dataframe
    profiles_data_df = pd.DataFrame(columns=['ID', 'Name', 'Nickname', 'Bio',
                                             'Post_Count', 'Follower_Count', 'Following_Count',
                                             'Is_Business', 'Is_Private', 'Is_Verified'])

    for profile_id in profiles_id_lst:
        profile_json = api_connector.get_profile_data_by_id(profile_id)
        profiles_data_df = profiles_data_df.append({
            'ID': profile_json.get("id", None),
            'Name': profile_json.get("full_name", None),
            'Nickname': profile_json.get("username", None),
            'Bio': profile_json.get("biography", None),
            'Post_Count': profile_json.get("posts_count", -1),
            'Follower_Count': profile_json.get("followers_count", -1),
            'Following_Count': profile_json.get("followings_count", -1),
            'Is_Business': profile_json.get("is_business_account", "unknown"),
            'Is_Private': profile_json.get("is_private", "unknown"),
            'Is_Verified': profile_json.get("is_verified", "unknown")}, ignore_index=True)

    # saving snowball iteration that each profile was found in
    profiles_data_df['Snowball_Iteration'] = snowball_iteration

    return profiles_data_df


def calculate_profiles_engagement(profiles_df):
    # extracting profiles id from received dataframe
    profiles_id_lst = list(profiles_df['ID'])

    for idx, profile_id in enumerate(profiles_id_lst):
        profile_engagement_during_last_month = 0

        logger.info("calculating profile with " + str(profile_id) + " id engagement. ("+str(idx)+"/("+str(len(profiles_id_lst))+")")
        # fetching all profile posts from last month
        one_month_ago_date = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        profile_posts_lst = api_connector.get_profile_posts(profile_id, MAX_AMOUNT, one_month_ago_date)

        # scanning all posts to discover who interacted with same profile id
        for post in profile_posts_lst:
            # adding the post engagement to the profile total engagement in the last month
            profile_engagement_during_last_month += post['likes_count']
            profile_engagement_during_last_month += post['comments_count']

        # updating profile engagement in relevant dict
        profiles_amount_of_posts_last_month_dict[profile_id] = len(profile_posts_lst)
        profile_engagement_last_month_dict[profile_id] = profile_engagement_during_last_month

    # updating profiles dict with engagement columns
    profiles_df['Last_Month_Posts'] = profiles_df['ID'].map(profiles_amount_of_posts_last_month_dict)
    profiles_df['Last_Month_Engagement'] = profiles_df['ID'].map(profile_engagement_last_month_dict)

    return profiles_df


def main():
    # read initial profiles
    read_origin_profiles_df()
    
    # snowballing 'SNOWBALL_ITERATIONS' times to find more profiles
    for idx in range(SNOWBALL_ITERATIONS):
        logger.info("Snowball iteration "+str(idx+1)+" has began ...")
        # extracting profiles id from relevant dataframe to snowball
        profile_id_lst = list(profiles_dfs_lst[idx]['ID'])
        # snowballing to get new list of profiles id
        new_profiles_id_from_snowball_lst = snowball(profile_id_lst)
        # adding profiles engagement to relevant profiles dataframe (we got this data after snowballing)
        profiles_dfs_lst[idx]['Last_Month_Posts'] = profiles_dfs_lst[idx]['ID'].map(profiles_amount_of_posts_last_month_dict)
        profiles_dfs_lst[idx]['Last_Month_Engagement'] = profiles_dfs_lst[idx]['ID'].map(profile_engagement_last_month_dict)
        # extracting new profiles data using Instagram API
        profiles_dfs_lst.append(extract_new_profiles_data(new_profiles_id_from_snowball_lst, idx + 1))

    # calculating the engagement of profiles that were found in the last iteration (we didn't snowball to discover engagement)
    profiles_dfs_lst[SNOWBALL_ITERATIONS] = calculate_profiles_engagement(profiles_dfs_lst[SNOWBALL_ITERATIONS])

    # merging all dataframe and saving the result into a csv file
    result_df = pd.concat(profiles_dfs_lst, ignore_index=True)
    result_df.to_csv(output_profiles_file_url)


if __name__ == "__main__":
    main()

