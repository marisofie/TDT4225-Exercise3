from DbConnector import DbConnector
from pprint import pprint
from datetime import datetime
from haversine import haversine

class Query:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def q1(self):
        collections = ["TrackPoint", "Activity", "User"]

        for collection in collections:
            col = self.db[collection]
            count = col.count()
            print("Collections {collection} has {count} documents.")

    def q3(self):
        """Find the top 20 users with the highest number of activities."""
        activities = self.db["Activity"]
        top20_users = activities.aggregate([
            {"$group": {
                "_id": "$user_id", "count": {"$sum": 1}
            }},
            {"$sort": {
                "count": -1}
            },
            {"$limit": 20}

        ])
        print("Users with the most activities are:")
        print("User  Activities")
        for user in top20_users:
            pprint(str(user["_id"]) + "  " + str(user["count"]))

    def q4(self):
        collection = self.db["Activity"]
        user_ids = collection.distinct("user_id", {"transportation_mode": "taxi"})
        print("The following users have taken a taxi:",user_ids)

    def q6(self):
        """a) Find the year with the most activities.
           b) Is this also the year with most recorded hours?"""
        activities = self.db["Activity"]
        #a
        activities_per_year = activities.aggregate([
            {"$group" : {
                "_id" : {"$year": "$start_date_time"},
                "Activities" : {"$sum" : 1}
            }},
            {"$sort": {
                "Activities": -1
            }},
            {"$limit": 1}
        ])
        for year in activities_per_year:
            print("The year with the most activities was: " + str(year["_id"]))

        #b
        hours_per_year = activities.aggregate([
            {"$group": {
                "_id" : {"$year": "$start_date_time"},
                "Hours" : {"$sum": {"$subtract":["$end_date_time", "$start_date_time"]} }
            }},
            {"$sort": {
                "Activities": -1
            }},
            {"$limit": 1}
        ])
        for year in hours_per_year:
            print("The year with the most hours was: " + str(year["_id"]))
    def q7(self):
        activity_col = self.db["Activity"]
        activities = activity_col.aggregate([
            {"$match": {
                "transportation_mode": "walk",
                "user_id": "112",
                "start_date_time": {"$gte": datetime(year=2008, month=1, day=1)},
                "end_date_time": {"$lt": datetime(year=2009, month=1, day=1)}
            }},
            {"$lookup": {
                "from": "TrackPoint", "localField": "_id", "foreignField": "activity_id", "as": "trackpoints"
            }},
            {"$project": {
                "trackpoints.lat": 1,
                "trackpoints.lon": 1
            }}
        ])

        total_dist = 0
        for activity in activities:
            trackpoints = activity["trackpoints"]
            for i in range(len(trackpoints)-1):
                coord1 = (float(trackpoints[i]["lat"]), float(trackpoints[i]["lon"]))
                coord2 = (float(trackpoints[i+1]["lat"]), float(trackpoints[i+1]["lon"]))
                dist = haversine(coord1, coord2)
                total_dist += dist

        print("Total distance walked by user 112: {total_dist}")

    def q9(self):
        """Find all users who have invalid activities, and the number of invalid activities per
        user
        An invalid activity is defined as an activity with consecutive trackpoints
        where the timestamps deviate with at least 5 minutes."""
        collection = self.db["Activity"]
        activity_trackpoints = collection.aggregate([
            {"$lookup": {
                "from": "TrackPoint",
                "localField" : "_id",
                "foreignField" : "activity_id",
                "as" : "Activity"
            }},
            {"$project": {
                "user_id" : "$user_id", "Activity" : "$Activity.activity_id" , "Time" : "$Activity.date_time"
            }},
            {"$limit" : 100}

        ])

        activity_trackpoints = list(activity_trackpoints)

        previous_time = datetime.now()
        previous_activity = 0
        invalid_trackpoints = {}

        for activity in activity_trackpoints:
            current_activity = activity["Activity"]

            for time in activity["Time"]:
                current_time = time

                if (current_activity == previous_activity) and (current_time - previous_time).total_seconds() > 5*60 :
                    if activity["user_id"] not in invalid_trackpoints.keys():
                        invalid_trackpoints[activity["user_id"]] = 1
                    else:
                        invalid_trackpoints[activity["user_id"]] += 1
                    break
                previous_time = current_time
                previous_activity = current_activity


        #for activity in activity_trackpoints:
            #pprint(activity)
        for user,value in invalid_trackpoints.items():
            print(user, value)



    def q10(self):
        lat = 39.916
        lon = 116.397
        collection = self.db["TrackPoint"]
        user_ids = collection.aggregate([
            {"$match": {
                "lat": {"$lt": lat + 0.0005, "$gte": lat - 0.0005},
                "lon": {"$lt": lon + 0.0005, "$gte": lon - 0.0005}
            }},
            {"$group": {
               "_id": "$user_id",
            }}
        ])

        print("The following users have been in the Forbidden City:")
        for data in user_ids:
            pprint(data["_id"])


def main():
    program = Query()
    program.q9()

main()
