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

    def q2(self):
        activities = self.db["Activity"].count_documents({})
        users = self.db["User"].count_documents({})
        avg = activities / users
        print("The average number of activities per user is: {}".format(round(avg, 0), float))

    def q4(self):
        collection = self.db["Activity"]
        user_ids = collection.distinct("user_id", {"transportation_mode": "taxi"})
        print("The following users have taken a taxi:", user_ids)

    def q5(self):
        collection = self.db["Activity"]
        types = collection.aggregate([
            {
                "$match": {
                    "transportation_mode": {
                        "$exists": True,
                        "$ne": "null",
                    }
                }
            },
            {

                "$group": {
                    "_id": "$transportation_mode",
                    "count": {"$sum": 1}
                }
            }
        ])
        print("Activities per transport mode:")
        for data in list(types):
            print(" - {}: {}".format(data["_id"], data["count"]))

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
            for i in range(len(trackpoints) - 1):
                coord1 = (float(trackpoints[i]["lat"]), float(trackpoints[i]["lon"]))
                coord2 = (float(trackpoints[i + 1]["lat"]), float(trackpoints[i + 1]["lon"]))
                dist = haversine(coord1, coord2)
                total_dist += dist

        print(f"Total distance walked by user 112: {total_dist}")

    def q8(self):
        collection = self.db["TrackPoint"]
        meters = collection.aggregate([
            {
                "$match": {
                    "user_id": {
                        "$exists": True,
                        "$ne": "null",
                    },
                    "altitude": {
                        "$exists": True,
                        "$ne": "null",
                    }
                }
            },
            {
                "$addFields": {
                    "prevTPID": {"$subtract": ["$_id", 1]}
                }
            },
            {
                "$sort": {
                    "_id": -1,
                }
            },
            {
                "$lookup": {
                    "from": "TrackPoint",
                    "localField": "prevTPID",
                    "foreignField": "_id",
                    "as": "t2"
                }
            },
            {
                "$project": {
                    "user_id": 1,
                    "delta": {"$subtract": ["$altitude", {"$sum": "$t2.altitude"}]}
                }
            },
            {
                "$group": {
                    "_id": '$user_id',
                    "total": {
                        "$sum": {
                            "$cond": [
                                {"$gt": ['$delta', 0]}, '$delta', 0
                            ]
                        }
                    }
                }
            },
            {
                "$sort": {"total": -1}
            },
            {
                "$limit": 20
            }
        ], allowDiskUse=True)
        print("Top 20 users that gained most altitude:")
        for data in list(meters):
            print(" {}: {}".format(data["_id"], float(data["total"]) * 0.3048))

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

    def q11(self):
        collection = self.db["Activity"]
        data = collection.aggregate([
            {
                "$match": {
                    "transportation_mode": {
                        "$exists": True,
                        "$ne": "null",
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "category": "$transportation_mode",
                        "user": "$user_id"
                    },
                    "count": {
                        "$sum": 1
                    },
                }
            },

            {
                "$sort": {
                    "count": -1,
                    "_id.category": -1
                },
            },
            {
                "$group": {
                    "_id": "$_id.user",
                    "count": {
                        "$first": "$count"
                    },
                    "category": {
                        "$first": "$_id.category"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "user_id": "$_id",
                    "most_used_transportation_mode": "$category",
                }
            },
            {
                "$sort": {
                    "user_id": 1
                }
            }
        ])
        print("Most common transportation mode for users (only users who have registered the transportation_mode)")
        print(list(data))

q = Query()
q.q8()

