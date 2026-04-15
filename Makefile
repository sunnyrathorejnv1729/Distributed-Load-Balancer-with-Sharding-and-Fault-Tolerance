# Checking if network exists
check_network:
	if [ -z "$$(sudo docker network ls -q -f name=my_network)" ]; then \
		echo "Network my_network does not exist. Creating..."; \
		sudo docker network create my_network; \
	else \
		echo "Network my_network already exists."; \
	fi

run:  check_network
	docker rm -f $(docker ps -aq)
	docker volume rm persistentStorage
	docker build -t loadbalancer ./loadbalancer
	docker build -t shard_manager ./Shard_Manager
	docker build -t server ./Server
	python spawner.py & 
	docker run -d -p 5000:5000 --privileged=true --name my_loadbalancer_app --network my_network -it loadbalancer
	docker run -d --privileged=true -v persistentStorage:/persistentStorageMedia --name shard_manager --network my_network -it shard_manager
