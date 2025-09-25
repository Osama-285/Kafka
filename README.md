For all confluent tools here is the complete docker file link:
https://github.com/confluentinc/cp-all-in-one/blob/8.0.0-post/cp-all-in-one/docker-compose.yml 

To Cheack List of Brockers:
command 1: docker exec -it <broker_container_name> bash
command 2: kafka-topics --bootstrap-server localhost:PORT --list

To Create New Topic:
docker exec -it <broker_container_name> kafka-topics --create   --topic <Topic Name>   --bootstrap-server localhost:9094   
--partitions 3   --replication-factor 1

To Delete a Broker:
docker exec -it <broker_container_name> kafka-topics --delete --topic <Topic Name> --bootstrap-server localhost:9094                                      
