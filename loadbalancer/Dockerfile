# Using Ubuntu 20.04 as base image
FROM python:3.9

# Setting the working directory to '/server'
WORKDIR /dockerLoadBalancer

# Copying current directory content to '/server'
COPY . /dockerLoadBalancer

RUN pip install -r requirements.txt

# Exposing port 5000
EXPOSE 5000

# Running the server
ENTRYPOINT python3 load_balancer.py
