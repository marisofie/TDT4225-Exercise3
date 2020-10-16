from DbConnector import DbConnector
from datetime import datetime

import os

DATASET_ROOT_PATH = "./dataset"
DATASET_PATH = DATASET_ROOT_PATH + "/Data/"
DATASET_LABELED_IDS = DATASET_ROOT_PATH + "/labeled_ids.txt"

class DataUploader:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

        self.ACTIVITY_ID = 1
        self.TRACKPOINT_ID = 1

    def create_collections(self):
        '''
        Create collections in database
        '''
        user_collection = self.db.create_collection('User')
        activity_collection = self.db.create_collection('Activity')
        trackpoint_collection = self.db.create_collection('TrackPoint')
        print('Created collections: ', (user_collection, activity_collection, trackpoint_collection))

    def insert_data_many(self, collection, data):
        '''
        Method for inserting many documents into a collection
        '''

        self.db[collection].insert_many(data)

    def get_labeled_ids(self):
        '''
        Read the file with users that has labeled data
        @return: users with labeled ids
        '''
        with open(DATASET_LABELED_IDS) as file:
            ids = file.readlines()
            ids = [id.strip() for id in ids]
            return ids

    def read_datetime(self, date_text):
        '''
        Convert date string to python date object
        @param date_text: date string
        @return: date python object
        '''
        date_text = date_text.replace('/', '-')
        return datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')

    def get_trackpoints_and_activites(self, root, plt_files, label_file, user_id):
        '''
        Get trackpoints and activies for a single user, match labeled activities to an activity derived from the trackpoints
        :param root: root path for directory
        :param plt_files: filename for plt file with trajectory data for an activity
        :param label_file: file directory and name for file with labeled activies with transport mode
        :param user_id: user id for the specific trajectories
        :return: trackpoints and activities for a user
        '''

        activities_with_labels = []
        activities = []
        trackpoints = []

        if label_file != "":
            with open(label_file) as file:
                    records = file.readlines()[1:]
                    for record in records:
                        items = [item.strip() for item in record.split('\t')]
                        items[0] = self.read_datetime(items[0])
                        items[1] = self.read_datetime(items[1])
                        activities_with_labels.append(items)

        for file_path in plt_files:
            if self.get_trackpoints(root + "/" + file_path) is not None:
                start_time, end_time, single_trackpoints = self.get_trackpoints(root + "/" + file_path)
                if len(activities_with_labels) > 0:
                    for activity in activities_with_labels:
                        if activity[0] == start_time and activity[1] == end_time:
                            activities.append({
                                "id": self.ACTIVITY_ID,
                                "user_id": user_id,
                                "transportation_mode": activity[2],
                                "start_date_time": start_time,
                                "end_date_time": end_time
                            })

                            self.ACTIVITY_ID += 1
                else:
                    activities.append({
                                "id": self.ACTIVITY_ID,
                                "user_id": user_id,
                                "transportation_mode": "null",
                                "start_date_time": start_time,
                                "end_date_time": end_time
                            })
                    self.ACTIVITY_ID += 1

                trackpoints.extend(single_trackpoints)

        return trackpoints, activities


    def get_trackpoints(self, file_path):
        '''
        Get specific trackpoints for a plt file
        :param file_path: file path for the plt file
        :return: start and end time of trajectory + trackpoints data structured for database injection
        '''

        start_time = datetime(2200, 1, 1, 00, 00, 00)
        end_time = datetime(1900, 1, 1, 00, 00, 00)

        trackpoints = []

        with open(file_path) as file:
            records = file.readlines()[6:]
            if len(records) > 2500:
                return
            for record in records:
                items = record.strip().split(',')
                time = self.read_datetime((items[5] + ' ' + items[6]))

                if time < start_time:
                    start_time = time

                if time > end_time:
                    end_time = time

                trackpoints.append({
                    "id": self.TRACKPOINT_ID,
                    "activity_id": self.ACTIVITY_ID,
                    "lat": items[0],
                    "lon": items[1],
                    "altitude": items[3],
                    "date_days": items[4],
                    "date_time": time
                })

                self.TRACKPOINT_ID += 1

        return start_time, end_time, trackpoints

    def upload_data(self):
        '''
        Get the data from the files and upload it to the database
        '''

        labeled_ids = self.get_labeled_ids()

        users = []
        users_ids = []

        activites = []
        trackpoints = []

        for root, dirs, files in os.walk(DATASET_PATH, topdown=True):
            path_parts = root.split('/')
            label_file = ""
            if len(path_parts) < 4: # make sure to be inside user folder
                continue
            user_id = path_parts[3]

            print("Adding user data...\n")
            if user_id not in users_ids:
                users_ids.append(user_id)
                users.append(({"_id": user_id, "has_labels": user_id in labeled_ids}))

            if user_id in labeled_ids:
                label_file = DATASET_PATH + user_id + "/labels.txt"


            if "Trajectory" in root:
                files.sort()

                print("Adding activites and trackpoints for user: " + user_id + "\n")
                if label_file != "":
                    trackpoints_single, activities_single = self.get_trackpoints_and_activites(root, files, label_file, user_id)
                else:
                    trackpoints_single, activities_single = self.get_trackpoints_and_activites(root, files, "", user_id)

                activites.extend(activities_single)
                trackpoints.extend(trackpoints_single)

            print("Number of trackpoints: " + str(len(trackpoints)))

        print("Inserting data into Users")
        self.insert_data_many("User", users)

        print("Inserting data into Activity")
        self.insert_data_many("Activity", activites)

        print("Inserting data into TrackPoints")
        self.insert_data_many("TrackPoints", trackpoints)

    def drop_collections(self):
        collection = self.db['User']
        collection.drop()

        collection = self.db['Activity']
        collection.drop()

        collection = self.db['TrackPoint']
        collection.drop()

def main():
    program = None
    try:
        program = DataUploader()
        program.upload_data()
    except Exception as e:
        print("ERROR: Failed to use database:", e.with_traceback())
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()