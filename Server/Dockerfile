# Using Ubuntu 20.04 as base image
FROM python:3.9

# Setting the working directory to '/server'
WORKDIR /dockerServer

# Copying current directory content to '/server'
COPY . /dockerServer

# Installing python3, pip3 and other dependencies(flask)
# RUN apt-get update && apt-get install -y python3 python3-pip && pip3 install flask
RUN pip install -r requirements.txt

# Exposing port 5000
EXPOSE 5000

# Running the server
ENTRYPOINT python3 server.py