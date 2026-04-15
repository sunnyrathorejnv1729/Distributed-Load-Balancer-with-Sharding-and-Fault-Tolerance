from flask import Flask,jsonify,request
import os

app = Flask(__name__)

@app.route('/spawn', methods = ['POST'])
def spawn():
    payload = request.get_json()
    servers = payload['servers']
    for server in servers:
        os.system(f'docker run -d --network my_network --name {server}_db database')
        os.system(f'docker run -d --privileged=true -v persistentStorage:/persistentStorageMedia -e "MYSQL_HOST={server}_db" -e "SERVER_NAME={server}" --network my_network --name {server} server')

    return {},200

@app.route('/respawn', methods = ['POST'])
def respawn():
    payload = request.get_json()
    server = payload['server']
    try :
        output_stream = os.popen(f'docker start {server}')
        for line in output_stream:
            if "Error" in line.lower():
                return {}, 400
        return {}, 200 
    except : 
        return {},400

@app.route('/remove', methods = ['POST'])
def remove():
    payload = request.get_json()
    servers = payload['servers']
    for server in servers:
        os.system(f'docker rm -f {server}_db') # incomplete
        os.system(f'docker rm -f {server}') # incomplete
    return {},200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 7000))
    app.run(debug=True, host='0.0.0.0', port=port)