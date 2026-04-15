from flask import Flask,jsonify,request
import json
import time
import os
import platform
import requests
import socket
import sqlite3
import threading
import random

VOLUME_PATH = '/persistentStorageMedia/'

app = Flask(__name__)

all_shards = {} # Format : {server name : [shard list]}
all_servers = {} # Format : {shard : [server list]}
primary_servers = {} # Format : {shard : primary server}
database_schema = None # To store the schema when init is called 

def elect_primary(shard):
    max_seq = 0
    max_server = ''
    for server in all_servers[shard['Shard_id']]:
        if os.path.exists(VOLUME_PATH+server+'.json'):
            logfile = open(VOLUME_PATH+server+'.json')
            log = json.load(logfile)
            for seq_num in log:
                if log[seq_num]['shard_id'] == shard and int(seq_num) > max_seq:
                    max_seq = int(seq_num)
                    max_server = server
            logfile.close()
    if max_server:
        primary_servers[shard['Shard_id']] = max_server
    else:
        primary_servers[shard['Shard_id']] = random.choice(all_servers[shard['Shard_id']])


def update_log(server):
    print("Update Log called")
    logfile = open(VOLUME_PATH+server+'.json')
    log = json.load(logfile)
    logfile.close()
    for shard in all_shards[server]:
        primary_logfile = open(VOLUME_PATH+primary_servers[shard]+'.json')
        primary_log = json.load(primary_logfile)
        primary_logfile.close()
        max_seq_present = 0
        for seq_num in log:
            if log[seq_num]['shard_id'] == shard and int(seq_num) > max_seq_present:
                max_seq_present = int(seq_num)
            elif log[seq_num]['shard_id'] == shard and log[seq_num]['is_committed'] == 0:
                url = f"http://{server}:5000/{primary_log[seq_num]['operation_name']}"
                data = primary_log[seq_num]['log']
                data['isPrimary'] = False
                while True:
                    if primary_log[seq_num]['operation_name'] == 'updateRAFT':
                        response = requests.put(url, json=data)
                    elif primary_log[seq_num]['operation_name'] == 'writeRAFT':
                        # print(url)
                        response = requests.post(url, json=data)
                    elif primary_log[seq_num]['operation_name'] == 'delRAFT':
                        response = requests.delete(url, json=data)
                    if response.status_code == 200:
                        break
                # log[seq_num]['is_committed'] = 1
        max_seq = 0
        for seq_num in primary_log:
            if primary_log[seq_num]['shard_id'] == shard and int(seq_num) > max_seq_present:
                log[seq_num] = primary_log[seq_num]
                url = f"http://{server}:5000/{primary_log[seq_num]['operation_name']}"
                data = primary_log[seq_num]['log']
                data['isPrimary'] = False
                while True:
                    if primary_log[seq_num]['operation_name'] == 'updateRAFT':
                        response = requests.put(url, json=data)
                    elif primary_log[seq_num]['operation_name'] == 'writeRAFT':
                        # print(url)
                        response = requests.post(url, json=data)
                    elif primary_log[seq_num]['operation_name'] == 'delRAFT':
                        response = requests.delete(url, json=data)
                    if response.status_code == 200:
                        break
    #             log[seq_num]['is_committed'] = 1
    # logfile_write = open(VOLUME_PATH+server+'.json','w')
    # json.dump(log,logfile_write)
    # logfile_write.close()
    print('Log updated')

# This is called upon Server respawned after Failure 
def replicate_log(server):
    print("Replicate Log called")
    log = {}
    try:
        for shard in all_shards[server]:
            if shard in primary_servers:
                if os.path.exists(VOLUME_PATH+primary_servers[shard]+'.json'):
                    primary_logfile = open(VOLUME_PATH+primary_servers[shard]+'.json')
                    primary_log = json.load(primary_logfile)
                    primary_logfile.close()
                    for seq_num in primary_log:
                        if primary_log[seq_num]['shard_id'] == shard:
                            log[seq_num] = primary_log[seq_num]
                            url = f"http://{server}:5000/{primary_log[seq_num]['operation_name']}"
                            data = primary_log[seq_num]['log']
                            data['isPrimary'] = False
                            while True:
                                if primary_log[seq_num]['operation_name'] == 'updateRAFT':
                                    response = requests.put(url, json=data)
                                elif primary_log[seq_num]['operation_name'] == 'writeRAFT':
                                    # print(url)
                                    response = requests.post(url, json=data)
                                elif primary_log[seq_num]['operation_name'] == 'delRAFT':
                                    response = requests.delete(url, json=data)
                                if response.status_code == 200:
                                    break
    except:
        print('Replication Failed')

@app.route('/init', methods=['GET'])
def init():
    # host_ip = socket.gethostbyname('host.docker.internal')
    try :
        # if platform.system() == 'Windows':
        host_ip = socket.gethostbyname('host.docker.internal')
        # else:
        #     host_ip = "172.17.0.1"
        url = f'http://{host_ip}:7000/spawn'
        payload = request.get_json()
        global database_schema
        N = payload.get('N')
        database_schema = payload.get("schema")
        shards = payload.get("shards")
        servers = payload.get("servers")

        for shard in shards:
            all_servers[shard["Shard_id"]] = []

        # Create the servers first 
        for server_name, shard_list in servers.items(): 
            data = {
                'servers' : [server_name]
            }
            response = requests.post(url, json=data)
            server_url = f"http://{server_name}:5000/config"
            data = {
                "schema" : database_schema, 
                "shards" : shard_list
            }
            # Wait for the database to be configured 
            while True :
                try : 
                    response = requests.post(server_url, json = data)
                    if response.status_code == 200 :
                        print("Server Spawn Success")
                        break
                except Exception as e :
                    print(e)
                    time.sleep(30)
            if server_name not in all_shards : 
                all_shards[server_name] = []
            all_shards[server_name].extend(shard_list)
            for shard_id in shard_list : 
                all_servers[shard_id].append(server_name)

        for shard in shards: 
            elect_primary(shard)
        start_health_check_thread()
    except Exception as e :
        print(f"------------------\n{e}")
    return {},200

@app.route('/add', methods=['GET'])
def add():
    payload = request.get_json()
    n = payload['n']
    new_shards = payload.get('new_shards')
    new_servers = payload.get('servers')

    # host_ip = socket.gethostbyname('host.docker.internal')
    host_ip = socket.gethostbyname('host.docker.internal')
    url = f'http://{host_ip}:7000/spawn'


    for shard in new_shards:
        all_servers[shard['Shard_id']] = []
        # We do not need to elect leader when adding new server 
        # TODO 
        # elect_primary(shard)

    for server_name, shard_list in new_servers.items(): 
        data = {
            'servers' : [server_name]
        }
        response = requests.post(url, json=data)
        server_url = f"http://{server_name}:5000/config"
        data = {
            "schema" : database_schema, 
            "shards" : shard_list
        }

        # Wait for the database to be configured 
        while True :
            try : 
                response = requests.post(server_url, json = data)
                if response.status_code == 200 :
                    print(f"Server {server_name} spawned successfully")
                    break
            except Exception as e :
                time.sleep(30)
        if server_name not in all_shards : 
            all_shards[server_name] = []
        all_shards[server_name].extend(shard_list)
        for shard_id in shard_list : 
            all_servers[shard_id].append(server_name)

        # Replicate the shards logs in the server
        replicate_log(server_name)
    for shard in new_shards:
        elect_primary(shard)
    return {},200

@app.route('/rm', methods=['GET'])
def rm():
    payload = request.get_json()
    deleted_servers = payload.get("servers")
    try:
        # host_ip = socket.gethostbyname('host.docker.internal')
        # print(all_shards)
        host_ip = socket.gethostbyname('host.docker.internal')
        url = f'http://{host_ip}:7000/remove'
        data = payload
        response = requests.post(url, json=data)
        print('Remove request successful:', response.text)
        for shard_id, primary_server in primary_servers.items() : 
            if primary_server in deleted_servers : 
                elect_primary({'Shard_id':shard_id})

            for server_name in deleted_servers : 
                try : 
                    all_servers[shard_id].remove(server_name)
                except Exception as e: 
                    pass

        for server_name in deleted_servers : 
            del all_shards[server_name]
        
        response.raise_for_status()
        status = 200
    except requests.exceptions.RequestException as e:
        data = {'message':f'Add failed with error : {e}'}
        status = 500
    return {},status

@app.route('/get_primary', methods=['POST'])
def get_primary():
    # print(primary_servers)
    try  :
        servers_list = dict(primary_servers)
        for shard_id_, server_name in servers_list.items() :
            if not check_server_health(f"http://{server_name}:5000/"):
                elect_primary({'Shard_id':shard_id_})
        return primary_servers,200
    except : 
        return {}, 400

# Server endpoint for requests at http://localhost:5000/home, methond=GET
@app.route('/home', methods = ['GET'])
def home():
    # Dictionary to return as a JSON object
    serverHomeMessage =  {"message": "Hello from Shard Manager",
                          "status": "successfull"}
    # Returning the JSON object along with the status code 200
    return serverHomeMessage, 200


# Health checkup portion --------------------------------------------------------------------------------------------------

def check_server_health(server_url):
    try:
        response = requests.get(f"{server_url}heartbeat", timeout=2)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def health_check():
    try:
        while True:
            time.sleep(240)
            servers_copy = dict(all_shards)
            for server_name, shard_list in servers_copy.items():
                if not check_server_health(f"http://{server_name}:5000/"):
                    print(f'{server_name} issue found!')
                    for shard_, p_server in primary_servers.items(): 
                        if server_name == p_server : 
                            print(f'{server_name} primary for {shard_}, electing new primary...')
                            elect_primary({'Shard_id':shard_})
                            print(f'New primary elected.')
                    host_ip = socket.gethostbyname('host.docker.internal')
                    url = f'http://{host_ip}:7000/respawn'
                    data = {
                        "server" : server_name
                    }
                    print('Respawning...')
                    response = requests.post(url, json=data)
                    if response.status_code == 200 :
                        print('Respawn successful.')
                        time.sleep(30)
                        print('Updating log...') 
                        update_log(server_name)
                        print('Logs updated')
                    else :
                        print('Respawn failed!')
                        url = f'http://{host_ip}:7000/spawn'
                        data = {
                            'servers' : [server_name]
                        }
                        print('Spawning new server...')
                        response = requests.post(url, json=data)
                        server_url = f"http://{server_name}:5000/config"
                        data = {
                            "schema" : database_schema, 
                            "shards" : shard_list
                        }
                        # Wait for the database to be configured 
                        while True :
                            try : 
                                response = requests.post(server_url, json = data)
                                if response.status_code == 200 :
                                    print("Server Spawn Success")
                                    break
                            except Exception as e :
                                # print(e)
                                time.sleep(10)
                        replicate_log(server_name)
    except Exception as e:
        print(e)

def start_health_check_thread():
    health_check_thread = threading.Thread(target=health_check)
    health_check_thread.daemon = True
    health_check_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 6000))
    app.run(debug=True, host='0.0.0.0', port=port)