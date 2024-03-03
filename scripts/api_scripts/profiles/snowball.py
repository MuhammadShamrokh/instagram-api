from APIConnectors.Data365Connector import Data365Connector


def read_profiles_id():
    return list()


def snowball(origin_profiles_list):
    return list()


def store_profiles_data_in_database(profiles_list):
    pass


def main():
    profiles_data_list = list()
    api_connector = Data365Connector()
    # read initial profiles
    initial_profiles_list = read_profiles_id()
    # snow ball to get bigger list of profiles
    profiles_id_found_using_snowball = snowball(initial_profiles_list)
    # get all data about profiles
    for profile_id in profiles_id_found_using_snowball:
        profiles_data_list.append(api_connector.get_profile_data_by_id(profile_id))
    # store profiles in database
    store_profiles_data_in_database(profiles_data_list)
    pass


if __name__ == "__main__":
    main()