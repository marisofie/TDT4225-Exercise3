from DbConnector import DbConnector

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
