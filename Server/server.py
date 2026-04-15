# Implementation of the server

from flask import Flask, request
import os
# use mysql-connector-python
from mysql import connector as ce
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text
from sqlalchemy import Table
import logging
from threading import Lock
import requests
import json
import threading

# Lock to handle the concurrency issues
lock = Lock()

#creating the Flask class object
app = Flask(__name__)

# Log operations
LOG_OPERATION_WRITE = "writeRAFT"
LOG_OPERATION_UPDATE = "updateRAFT"
LOG_OPERATION_DELETE = "delRAFT"

# Environment Variables to cnnect to database. If not present, use the default values
DATABASE_USER = os.environ.get('MYSQL_USER', 'root')
DATABASE_PASSWORD = os.environ.get('MYSQL_PASSWORD', 'abc')
DATABASE_DB = os.environ.get('MYSQL_DATABASE', 'shardsDB')
DATABASE_HOST = os.environ.get('MYSQL_HOST', 'localhost')
DATABASE_PORT = os.environ.get('MYSQL_PORT', '3306')

app.config['SQLALCHEMY_DATABASE_URI'] = f'mysql+mysqlconnector://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_DB}'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# For local testing
# app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+mysqlconnector://root:password@localhost:3306/shardsDB'

# Creating the SQLALchemy object
db = SQLAlchemy(app)

# dictionary to store the logs
logs = {}
serverFileName = None
VOLUME_PATH = '/persistentStorageMedia/'
log_lock = threading.Lock()

# ORM Model for the Student table. Table name will be dynamically provided
def ClassFactory(name):
    # Check if the model already exists in the global namespace
    existing_model = globals().get(name)
    
    if existing_model is not None:
        return existing_model
    
    # If the model does not exist, create it
    tabledict = {
        'Stud_id': db.Column(db.Integer, primary_key=True),
        'Stud_name': db.Column(db.String(100)),
        'Stud_marks': db.Column(db.String(100))
    }

    new_model = type(name, (db.Model,), {'__tablename__': name, **tabledict})
    
    # Assign the newly created model to the global namespace
    globals()[name] = new_model
    
    return new_model


# Server endpoint for requests at http://localhost:5000/home, methond=GET
@app.route('/home', methods = ['GET'])
def home():
    # Server ID taking from the environment variable named SERVER_ID
    serverID = os.environ.get('SERVER_ID')

    # Dictionary to return as a JSON object
    serverHomeMessage =  {"message": "Hello from Server: [" + str(serverID) + "]",
                          "status": "successfull"}

    # Returning the JSON object along with the status code 200
    return serverHomeMessage, 200

# Server endpoint for requests at http://localhost:5000//heartbeat, method=GET
@app.route('/heartbeat', methods = ['GET'])
def heartbeat():
    # Returning empty response along with status code 200
    return "", 200

# Server endpoint for requests at http://localhost:5000/config, methond=POST
@app.route('/config', methods = ['POST'])
def config():
    message = {}
    statusCode = 0

    try:
        # Getting the schema and shards from the payload
        payload = request.get_json()
        schema = payload.get('schema')
        shards = payload.get('shards')

        # checking if the schema and shards are present in the payload
        message["message"] = ""
        isError = False
        # Server ID taking from the environment variable named SERVER_ID

        serverName = DATABASE_HOST

        if schema is None or shards is None:
            isError = True
            
        else:
            # Getting 'columns' and 'dtypes' from the schema
            columns = schema.get('columns')
            dtypes = schema.get('dtypes')

            # Checking if the columns and dtypes are present in the schema
            if columns is None or dtypes is None:
                isError = True
            else:
                # Creating the shards in the database
                for shard in shards:
                    # Check if the table already exists in metadata
                    # existing_table = db.Model.metadata.tables.get(shard)
                    # if existing_table is not None:
                    #     message["message"] += serverName + ":" + shard + "(existing), "
                    #     continue
                    # Creating the table in the database
                    table = ClassFactory(shard)
                    db.create_all()
                    db.session.commit()
                    message["message"] += serverName + ":" + shard + ", "

                # Returning the success message along with the status code 200
                # Remove the last comma from the message
                message["message"] = message["message"][:-2]
                message["message"] += "configured"
                message["status"] = "success"
                statusCode = 200
            
        # If the schema or shards are not present in the payload
        if isError:
            message["message"] = "Invalid Payload"
            message["status"] = "Unsuccessfull"
            statusCode = 400
    except Exception as e:
        message["message"] = "Error: " + str(e)
        message["status"] = "Unsuccessfull"
        statusCode = 400

        # Returning the error message along with the status code 400
    print(message,statusCode)
    return message, statusCode

def executeAndReturn(query):
    try:
        # start a transaction
        db.session.begin()
        
        # Execute the query using SQLAlchemy's session
        executionResult = db.session.execute(text(query))
        # Commit the transaction
        db.session.commit()
        # end the transaction
        db.session.close()

        result = executionResult.fetchall()

        if result is None or len(result) == 0:
            result = []
        return result
    except SQLAlchemyError as e:
        # Rollback the transaction in case of an error
        db.session.rollback()
        # throw the exception to the calling function
        raise e

# endpoint to show tables. Not in the assignment. Used for testing
@app.route('/showTables', methods = ['GET'])
def showTables():
    message = {}
    statusCode = 0

    try:
        # Query to get the list of tables in the database
        query = "SHOW TABLES"

        # Execute the query using SQLAlchemy's session
        result = executeAndReturn(query)

        # List to store the tables
        tables = []

        # Iterating through the result and storing the tables in the list only if result is not None or is not empty
        for row in result:
            tables.append(row[0])

        message = {"tables": tables, "status": "success"}
        statusCode = 200
    except Exception as e:
        # Message with error description
        message = {"message": "Error: " + str(e), "status": "Unsuccessfull"}
        statusCode = 400
    
    return message, statusCode

# # Server endpoint for requests at http://localhost:5000/copy, methond=GET
@app.route('/copy', methods = ['GET'])
def copy():
    message = {}
    statusCode = 0
    
    try:
        # Getting the list of shard tables from the payload
        payload = request.get_json()
        shards = payload.get('shards')

        # Use ORM to get the data entries from the shard tables. If table is empty, return empty list
        for shard in shards:
            message[shard] = []
            table = ClassFactory(shard)
            query = db.session.query(table).all()
            for row in query:
                message[shard].append({"Stud_id":row.Stud_id, "Stud_name":row.Stud_name, "Stud_marks":row.Stud_marks})
            
        message["status"] = "success"
        statusCode = 200
    except Exception as e:
        message = {"message": "Error: " + str(e), "status": "Unsuccessfull"}
        statusCode = 400

    # Returning the dictionary along with the status code 200
    return message, statusCode

def getRequestURL(server, endpoint):
    return "http://" + server + ":5000/" + endpoint

# Function to assign the logId
def assignLogIdAndFileName():
    # Check if serverFileName exists. If exists, then assign the maximum logId + 1
    global serverFileName

    if serverFileName == None:
        serverFileName = os.environ.get('SERVER_NAME') + '.json'
    
    if not os.path.exists(VOLUME_PATH + serverFileName):
        with open(VOLUME_PATH + serverFileName, 'w') as f:
            json.dump({},f)

# Function to write the log
def writeLog(operationName, log, log_id, shard_id, is_commited):
    # make logId as key, operationName and log as values
    dataToWrite = {}
    
    dataToWrite[log_id] = {"operation_name": operationName, "log": log, "shard_id" : shard_id, "is_committed" : is_commited}
    log_lock.acquire()
    if os.path.exists(VOLUME_PATH + serverFileName):
        with open(VOLUME_PATH + serverFileName, 'r') as f:
            data = json.load(f)
            data.update(dataToWrite)
    else:
        data = dataToWrite
    with open(VOLUME_PATH + serverFileName, 'w') as f:
        json.dump(data, f)
    log_lock.release()

# Function to return the logs
@app.route('/getLogs', methods = ['GET'])
def getLogs():
    # Read the logs from the volume
    message = {}
    statusCode = 0
    try:
        logs = {}

        if os.path.exists(VOLUME_PATH + serverFileName):
            with open(VOLUME_PATH + serverFileName, 'r') as f:
                logs = json.load(f)
        else:
            logs = {}
        message = {"logs": logs, "status": "success"}
        statusCode = 200
    except Exception as e:
        logs = {"message": "Error: " + str(e), "status": "Unsuccessfull"}
        statusCode = 400
    
    return message, statusCode

def writeData(shard, curr_idx, data):
    # Write data to the database
    try:
        # List to store the data entries
        dataEntries = []
        duplicate = 0

        # Iterating through the data entries. Use ORM to insert the data. Also check for duplicate entries and entries that does not violate the integrity constraints
        for entry in data:
            # Check if the entry already exists in the shard
            table = ClassFactory(shard)
            query = db.session.query(table).filter_by(Stud_id=entry['Stud_id']).all()
            if len(query) > 0:
                duplicate += 1
                # If the entry already exists, skip the entry
                continue
            # If the entry does not exist, add the entry to the list
            dataEntries.append(table(Stud_id=entry['Stud_id'], Stud_name=entry['Stud_name'], Stud_marks=entry['Stud_marks']))
            print("Entry added")

        # Add the data entries to the shard table
        db.session.add_all(dataEntries)
        db.session.commit()
    except Exception as e:
        return e
    
    return len(dataEntries), duplicate


# Replicate write if server primary else just store the data
# Assume 'server1' is primary and 'server2' and 'server3' are replicas
# json payload: { "shard": "Shard1", 
#                 "curr_idx": 0,
#                 "log_id" : 0, 
#                 "data": [{"Stud_id": 1, "Stud_name": "Abc", "Stud_marks": 10}],
#                 "isPrimary": true,
#                 "otherServers": ["server2", "server3"]}
@app.route('/writeRAFT', methods = ['POST'])
def writeRAFT():
    message = {}
    statusCode = 0
    response = {}
    replicated = 0
    duplicate = 0
    entriesAdded = 0
    isReplicatedToMajority = False

    try:
        # Getting the shard, current index and data entries from the payload
        payload = request.get_json()
        shard = payload.get('shard')
        curr_idx = int(payload.get('curr_idx'))
        data = payload.get('data')
        log_id = payload.get('log_id')
        isPrimary = payload.get('isPrimary')
        otherServers = payload.get('otherServers')

        if isPrimary:
            # write to log and replicate to other servers
            writeLog(LOG_OPERATION_WRITE, payload, log_id, shard, 0)

            # request for other servers
            requestToReplica = {"shard": shard, 
                        "curr_idx": curr_idx,
                        "log_id" : log_id,
                        "data": data, 
                        "isPrimary": False, 
                        "otherServers": []}
            
            # send request to other servers
            for server in otherServers:
                url = getRequestURL(server, "writeRAFT")
                try : 
                    response = requests.post(url, json=requestToReplica)
                    if response.status_code == 200:
                        replicated += 1
                except : 
                    pass 

            # If replicated to majority of servers, write to the database
            if replicated >= len(otherServers) // 2:
                # writing to the database
                entriesAdded, duplicate = writeData(shard, curr_idx, data)
                isReplicatedToMajority = True
        else:
            # If not primary.
            # write to log
            writeLog(LOG_OPERATION_WRITE, payload, log_id, shard, 0)
            # writing to the database
            entriesAdded, duplicate = writeData(shard, curr_idx, data)

        # Returning the dictionary along with the status code 200
        if isPrimary:
            if isReplicatedToMajority:
                message["message"] = "Data entries added"
                message["current_idx"] = str(curr_idx + entriesAdded)
                if duplicate > 0:
                    if duplicate == len(data):
                        message["message"] = "No data entries added. All entries are duplicate"
                    else:
                        message["message"] += " (" + str(duplicate) + " duplicate entries skipped)"

                message["status"] = "success"
                statusCode = 200
                writeLog(LOG_OPERATION_WRITE, payload, log_id, shard, 1)
            else:
                message["message"] = "Data entries not added. Not replicated to majority of servers"
                message["status"] = "Unsuccessfull"
                statusCode = 400
        else:
            if entriesAdded > 0 or duplicate > 0:
                message["message"] = "Data entries added"
                message["status"] = "success"
                statusCode = 200
                writeLog(LOG_OPERATION_WRITE, payload, log_id, shard, 1)
    except Exception as e:
        message = {"message": "Error: " + str(e), "status": "Unsuccessfull"}
        statusCode = 400

    return message, statusCode
            

# Server endpoint for requests at http://localhost:5000/write, methond=POST
@app.route('/write', methods = ['POST'])
def write():
    message = {}
    statusCode = 0

    try:
        with lock:
            # Getting the shard, current index and data entries from the payload
            payload = request.get_json()
            shard = payload.get('shard')
            curr_idx = int(payload.get('curr_idx'))
            data = payload.get('data')
            duplicate = 0
            
            # List to store the data entries
            dataEntries = []

            # Iterating through the data entries. Use ORM to insert the data. Also check for duplicate entries and entries that does not violate the integrity constraints
            for entry in data:
                # Check if the entry already exists in the shard
                table = ClassFactory(shard)
                query = db.session.query(table).filter_by(Stud_id=entry['Stud_id']).all()
                if len(query) > 0:
                    duplicate += 1
                    # If the entry already exists, skip the entry
                    continue
                # If the entry does not exist, add the entry to the list
                dataEntries.append(table(Stud_id=entry['Stud_id'], Stud_name=entry['Stud_name'], Stud_marks=entry['Stud_marks']))
                print("Entry added")

            # Add the data entries to the shard table
            db.session.add_all(dataEntries)
            db.session.commit()
            

            # Returning the dictionary along with the status code 200
            message["message"] = "Data entries added"
            message["current_idx"] = str(curr_idx + len(dataEntries))
            if duplicate > 0:
                if duplicate == len(data):
                    message["message"] = "No data entries added. All entries are duplicate"
                else:
                    message["message"] += " (" + str(duplicate) + " duplicate entries skipped)"
            message["status"] = "success"
            statusCode = 200
    except Exception as e:
        message = {"message": "Error: " + str(e), "status": "Unsuccessfull"}
        statusCode = 400

    return message, statusCode

@app.route('/read', methods = ['POST'])
def read():
    message = {}
    statusCode = 0

    try:
        # Getting the shard, low and high from the payload
        payload = request.get_json()
        shard = payload.get('shard')
        low = int(payload.get('Stud_id').get('low'))
        high = int(payload.get('Stud_id').get('high'))

        # List to store the data entries
        data = []

        # Use ORM to get the data entries from the shard table. If table is empty, return empty list
        table = ClassFactory(shard)
        query = db.session.query(table).filter(table.Stud_id >= low, table.Stud_id <= high).all()
        for row in query:
            data.append({"Stud_id":row.Stud_id, "Stud_name":row.Stud_name, "Stud_marks":row.Stud_marks})
        message["data"] = data
        message["status"] = "success"
        statusCode = 200
    except Exception as e:
        message = {"message": "Error: " + str(e), 
                   "status": "Unsuccessfull"}
        statusCode = 400

    return message, statusCode

def updateData(shard, Stud_id, entry):
    # Update the data entry in the database
    try:
        # Use ORM to update the data entry in the shard table
        table = ClassFactory(shard)

        db.session.query(table).filter_by(Stud_id=Stud_id).update({
            "Stud_name" : entry['Stud_name'],
            "Stud_marks" : entry['Stud_marks']
        })
        db.session.commit()
    except Exception as e:
        return e
    
    return True

def isIdExists(shard, Stud_id):
    # Check if the Stud_id exists in the shard
    try:
        # Use ORM to get the data entry from the shard table
        table = ClassFactory(shard)
        query = db.session.query(table).filter_by(Stud_id=Stud_id).all()
        if len(query) == 0:
            return False
    except Exception as e:
        return e
    
    return True

# Replicate write if server primary else just store the data
# Assume 'server1' is primary and 'server2' and 'server3' are replicas
# Json= {"shard":"sh2",
#       "Stud_id":2255,
#       "log_id" : 0, 
#       "data": {"Stud_id":2255,"Stud_name":GHI,"Stud_marks":28},
#       "isPrimary":true,
#       "otherServers":["server2","server3"]
# }
@app.route('/updateRAFT', methods = ['PUT'])
def updateRAFT():
    message = {}
    statusCode = 0
    response = {}
    replicated = 0
    updated = False
    isReplicatedToMajority = False

    try:
        # Getting the shard, Stud_id and data entry from the payload
        payload = request.get_json()
        shard = payload.get('shard')
        Stud_id = int(payload.get('Stud_id'))
        log_id = payload.get('log_id')
        entry = payload.get('data')
        isPrimary = payload.get('isPrimary')
        otherServers = payload.get('otherServers')

        if not isIdExists(shard, Stud_id):
            message["message"] = "Nothing to update. Given ID does not exist."
            message["status"] = "success"
            statusCode = 200
        else:
            if isPrimary:
                # write to log and replicate to other servers
                writeLog(LOG_OPERATION_UPDATE, payload, log_id, shard, 0)

                # request for other servers
                requestToReplica = {"shard": shard, 
                            "Stud_id": Stud_id, 
                            "log_id" : log_id,
                            "data": entry, 
                            "isPrimary": False, 
                            "otherServers": []}
                
                # send request to other servers
                for server in otherServers:
                    url = getRequestURL(server, "updateRAFT")
                    try : 
                        response = requests.put(url, json=requestToReplica)
                        if response.status_code == 200:
                            replicated += 1
                    except : 
                        pass
                # If replicated to majority of servers, write to the database
                if replicated >= len(otherServers) // 2:
                    # writing to the database
                    updated = updateData(shard, Stud_id, entry)
                    isReplicatedToMajority = True
            else:
                # If not primary.
                # write to log
                writeLog(LOG_OPERATION_UPDATE, payload, log_id, shard, 0)
                # writing to the database
                updated = updateData(shard, Stud_id, entry)

            # Returning the dictionary along with the status code 200
            if isPrimary:
                if isReplicatedToMajority:
                    if updated:
                        message["message"] = "Data entry for Stud_id:" + str(Stud_id) + " updated"
                        message["status"] = "success"
                        statusCode = 200
                        writeLog(LOG_OPERATION_UPDATE, payload, log_id, shard, 1)
                    else:
                        message["message"] = "Error updating data entry"
                        message["status"] = "Unsuccessfull"
                        statusCode = 400
                else:
                    message["message"] = "Data entry not updated. Not replicated to majority of servers"
                    message["status"] = "Unsuccessfull"
                    statusCode = 400
            else:
                if updated:
                    message["message"] = "Data entry for Stud_id:" + str(Stud_id) + " updated"
                    message["status"] = "success"
                    statusCode = 200
                    writeLog(LOG_OPERATION_UPDATE, payload, log_id, shard, 1)
    except Exception as e:
        message = {"message": "Error: " + str(e), "status": "Unsuccessfull"}
        statusCode = 400

    return message, statusCode
            

# Server endpoint for requests at http://localhost:5000/update, methond=PUT
@app.route('/update', methods = ['PUT'])
def update():
    message = {}
    statusCode = 0
    
    try:
        with lock:
            payload = request.get_json()
            shard = payload.get('shard')
            Stud_id = int(payload.get('Stud_id'))
            entry = payload.get('data')

            table = ClassFactory(shard)
            query = db.session.query(table).filter_by(Stud_id=entry['Stud_id']).all()
            if len(query) == 0:
                message["message"] = "Nothing to update. Given ID does not exist."
            else:
                table(Stud_id=entry['Stud_id'], Stud_name=entry['Stud_name'], Stud_marks=entry['Stud_marks'])
                db.session.query(table).filter_by(Stud_id=entry['Stud_id']).update({
                    "Stud_name" : entry['Stud_name'],
                    "Stud_marks" : entry['Stud_marks']
                })
                db.session.commit()
                message["message"] = "Data entry for Stud_id:" + str(Stud_id) + " updated"

            message["status"] = "success"
            statusCode = 200
    except Exception as e:
        message = {"message": "Error: " + str(e),
                   "status": "Unsuccessfull"}
        statusCode = 400
    return message, statusCode

def deleteData(shard, Stud_id):
    # Delete the data entry from the database
    try:
        # Use ORM to delete the data entry from the shard table
        table = ClassFactory(shard)
        query = db.session.query(table).filter_by(Stud_id=Stud_id).delete()
        db.session.commit()
    except Exception as e:
        return e
    
    return True

# Assume 'server1' is primary and 'server2' and 'server3' are replicas
# Json= {"shard":"sh1",
#         "Stud_id":2255,
#         "isPrimary":true,
#         "otherServers":["server1","server2","server3"]
# }
@app.route('/delRAFT', methods = ['DELETE'])
def deleteRAFT():
    message = {}
    statusCode = 0
    response = {}
    replicated = 0
    deleted = False
    isReplicatedToMajority = False

    try:
        # Getting the shard and Stud_id from the payload
        payload = request.get_json()
        shard = payload.get('shard')
        Stud_id = int(payload.get('Stud_id'))
        log_id = payload.get('log_id')
        isPrimary = payload.get('isPrimary')
        otherServers = payload.get('otherServers')

        if not isIdExists(shard, Stud_id):
            message["message"] = "Nothing to delete. Given ID does not exist."
            message["status"] = "success"
            statusCode = 200
        else:
            if isPrimary:
                # write to log and replicate to other servers
                writeLog(LOG_OPERATION_DELETE, payload, log_id, shard, 0)

                # request for other servers
                requestToReplica = {"shard": shard, 
                            "Stud_id": Stud_id, 
                            "log_id" : log_id,
                            "isPrimary": False, 
                            "otherServers": []}
                
                # send request to other servers
                for server in otherServers:
                    url = getRequestURL(server, "delRAFT")
                    response = requests.delete(url, json=requestToReplica)

                    if response.status_code == 200:
                        replicated += 1

                # If replicated to majority of servers, write to the database
                if replicated >= len(otherServers) // 2:
                    # writing to the database
                    deleted = deleteData(shard, Stud_id)
                    isReplicatedToMajority = True
            else:
                # If not primary.
                # write to log
                writeLog(LOG_OPERATION_DELETE, payload, log_id, shard, 0)
                # writing to the database
                deleted = deleteData(shard, Stud_id)

            # Returning the dictionary along with the status code 200
            if isPrimary:
                if isReplicatedToMajority:
                    if deleted:
                        message["message"] = "Data entry for Stud_id:" + str(Stud_id) + " removed"
                        message["status"] = "success"
                        statusCode = 200
                        writeLog(LOG_OPERATION_DELETE, payload, log_id, shard, 1)
                    else:
                        message["message"] = "Error deleting data entry"
                        message["status"] = "Unsuccessfull"
                        statusCode = 400
                else:
                    message["message"] = "Data entry not removed. Not replicated to majority of servers"
                    message["status"] = "Unsuccessfull"
                    statusCode = 400
            else:
                if deleted:
                    message["message"] = "Data entry for Stud_id:" + str(Stud_id) + " removed"
                    message["status"] = "success"
                    statusCode = 200
                    writeLog(LOG_OPERATION_DELETE, payload, log_id, shard, 1)
    except Exception as e:
        message = {"message": "Error: " + str(e), "status": "Unsuccessfull"}
        statusCode = 400

    return message, statusCode

# Server endpoint for requests at http://localhost:5000/del, methond=DELETE
@app.route('/del', methods = ['DELETE'])
def delete():
    message = {}
    statusCode = 0

    try:
        # Getting the shard and Stud_id from the payload
        payload = request.get_json()
        shard = payload.get('shard')
        Stud_id = int(payload.get('Stud_id'))

        # Use ORM to delete the data entry from the shard table
        table = ClassFactory(shard)
        query = db.session.query(table).filter_by(Stud_id=Stud_id).delete()
        db.session.commit()
        message["message"] = "Data entry with Stud_id:" + str(Stud_id) + " removed"
        message["status"] = "success"
        statusCode = 200
    except Exception as e:
        message = {"message": "Error: " + str(e), 
                   "status": "Unsuccessfull"}
        statusCode = 400

    return message, statusCode

# Server endpoints for all other requests. Kind of error handler
@app.route('/', defaults={'path': ''}, methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD'])
@app.route('/<path:path>', methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD'])
def invalidUrlHandler(path):
    # Returning an error message stating the valid endpoints
    errorMessage = {"message": "Invalid Endpoint",
                    "Valid Endpoints": ["/heartbeat method='GET'",
                                        "/config method='POST'",
                                        "/copy method='GET'",
                                        "/write method='POST'",
                                        "/read method='POST'",
                                        "/update method='PUT'",
                                        "/del method='DELETE'"],
                    "status": "Unsuccessfull"}
    
    # Returning the JSON object along with the status code 404
    return errorMessage, 404
    
if __name__ == '__main__':
    # Assign the logId and serverFileName
    assignLogIdAndFileName()
    app.run(debug=True,host="0.0.0.0", port=5000)
