import json

def volumeTest():
    # Write some data in volume
    with open('/persistentStorageMedia/data.json', 'w') as f:
        json.dump({'key': 'value'}, f)

    # Read the data from volume
    with open('/persistentStorageMedia/data.json', 'r') as f:
        data = json.load(f)
        print(data)

# Call the function
volumeTest()
