import hashlib
import os

def hash_function(value):
    return int(hashlib.md5(value.encode()).hexdigest(), 16) % (2**32)



# id is a numeric value 
# def createServer(id):
#     container_name = f'server{id}'  # Adjust the naming convention as needed
#     os.popen(f"docker stop {container_name}")
#     # docker run --name container1 --network my_network -p 8080:5000 -d my_image1

#     return os.popen(f'docker run --name {container_name} -p 5010:5000 --network my_network -e SERVER_ID={id} -d serverimage').read()

def get_container_ip(container_name):
    return os.popen(f'sudo docker inspect -f "{{{{.NetworkSettings.Networks.my_network.IPAddress}}}}" {container_name}').read().strip()

def get_container_iD(container_name):
    return os.popen(f'sudo docker ps -aqf "name={container_name}"').read().strip()



def createServer(id, container_name, port):
    os.popen(f"sudo docker stop {container_name}")
    os.popen(f"sudo docker rm {container_name}")
    # print("Yooo")
    os.popen(f'sudo docker run -p {port}:5000 -e "SERVER_ID={id}" -e "MYSQL_USER=server" --network my_network -e "MYSQL_PASSWORD=abc" -e "MYSQL_DATABASE=shardsDB" -e "MYSQL_HOST={container_name}" -e "MYSQL_PORT=3306" --name {container_name} -d my-server-app')
    # os.popen(f'sudo docker run -p {port}:5000 -e "SERVER_ID={id}" -e "MYSQL_USER=server" -e "MYSQL_PASSWORD=abc" -e "MYSQL_DATABASE=shardsDB" -e "MYSQL_HOST=localhost" -e "MYSQL_PORT=3306" --name {container_name} -d my-server-app')
    print(f"Port : {port}")
    # return os.popen(f'sudo docker run --name {container_name} --network my_network -e SERVER_ID={id} -p {port}:5000 -d serverimage').read()
    # print(f"Created Server {container_name}")
    return 
    # return os.popen(f'sudo docker run --name {container_name} --network my_network -e SERVER_ID={id} -p {port}:5000 -d serverimage').read()