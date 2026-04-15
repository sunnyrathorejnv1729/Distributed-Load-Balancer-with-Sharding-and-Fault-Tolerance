# Using Ubuntu 20.04 as base image
FROM python:3.9

# Setting the working directory to '/server'
WORKDIR /dockerShardManager

# Copying current directory content to '/server'
COPY . /dockerShardManager

RUN pip install -r requirements.txt

# Exposing port 6000
EXPOSE 6000

# Running the server
ENTRYPOINT python3 shard_manager.py
