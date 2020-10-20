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
            print(f"Collections {collection} has {count} documents.")

    def q4(self):
        collection = self.db["Activity"]
        user_ids = collection.distinct("user_id", {"transportation_mode": "taxi"})
        print(user_ids)

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

        print(f"Total distance walked by user 112: {total_dist}")

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

