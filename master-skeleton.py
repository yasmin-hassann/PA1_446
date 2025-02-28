from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
import sys

#  Ensure workers are accessible via RPC
workers = {
    'worker-1': ServerProxy("http://localhost:23001/"),
    'worker-2': ServerProxy("http://localhost:23002/")
}

def safe_query(worker, method, *args):
    """Safely query a worker. If the worker is down, return an empty list."""
    try:
        return getattr(worker, method)(*args)
    except Exception as e:
        print(f" Warning: {worker} is unavailable. Returning empty response.")
        return []  # Return an empty list instead of crashing


def getbyname(name):
    """Routes query to the correct worker based on first letter of the name."""
    first_letter = name[0].lower()
    if "a" <= first_letter <= "m":
        return safe_query(workers['worker-1'], "getbyname", name)
    else:
        return safe_query(workers['worker-2'], "getbyname", name)

def getbylocation(location):
    """Sends query to both workers and merges results."""
    results1 = safe_query(workers['worker-1'], "getbylocation", location)
    results2 = safe_query(workers['worker-2'], "getbylocation", location)
    return results1 + results2  # Combine results

def getbyyear(location, year):
    """Sends location and year query to both workers and merges results."""
    results1 = safe_query(workers['worker-1'], "getbyyear", location, year)
    results2 = safe_query(workers['worker-2'], "getbyyear", location, year)
    return results1 + results2  # Combine results

if __name__ == "__main__":
    # Check if the script was started with a port argument
    if len(sys.argv) < 2:
        print(" Error: Missing port number. Usage: python3 master.py <PORT>")
        sys.exit(1)  # Stop execution if no port is provided

    port = int(sys.argv[1])  # Get master port from command-line argument

    # Create an XML-RPC server
    server = SimpleXMLRPCServer(("localhost", port), allow_none=True)

    #  Register RPC functions
    server.register_function(getbyname, "getbyname")
    server.register_function(getbylocation, "getbylocation")
    server.register_function(getbyyear, "getbyyear")

    print(f" Master is listening on port {port}...")
    server.serve_forever()

