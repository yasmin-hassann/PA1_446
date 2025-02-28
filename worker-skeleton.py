

from xmlrpc.server import SimpleXMLRPCServer
import json
import sys


# Load the data file based on worker type (am or nz)
def load_data(filename):
    with open(filename, 'r') as file:
        return json.load(file)


# Define Worker class
class Worker:
    def __init__(self, filename):
        self.data = load_data(filename)

    def getbyname(self, name):
        """Return person details if name exists, otherwise return error."""
        return self.data.get(name, {"error": True, "message": "No record found."})

    def getbylocation(self, location):
        """Return all people who lived in a given location."""
        return [record for record in self.data.values() if record["location"] == location]

    def getbyyear(self, location, year):
        """Return all people who lived in a given location in a specific year."""
        return [record for record in self.data.values() if record["location"] == location and record["year"] == year]


# Determine which worker to start
if __name__ == "__main__":
    port = int(sys.argv[1])  # Worker port (23001 or 23002)
    dataset = sys.argv[2]  # "am" or "nz"

    filename = f"data-{dataset}.json"  # e.g., data-am.json or data-nz.json
    worker = Worker(filename)

    server = SimpleXMLRPCServer(("localhost", port), allow_none=True)

    # Register functions for RPC calls
    server.register_function(worker.getbyname, "getbyname")
    server.register_function(worker.getbylocation, "getbylocation")
    server.register_function(worker.getbyyear, "getbyyear")

    print(f"Worker ({dataset}) is listening on port {port}...")
    server.serve_forever()
