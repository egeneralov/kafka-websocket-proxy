# kafka-websocket-proxy

(: just for fun

## how to use

- Put kafka online `docker-compose up -d`
- Wait for kafka to be ready
- create topic `kafka-topics --bootstrap-server 127.1:9092 --create --topic topicname`
- ensure topic created `kafka-topics --bootstrap-server 127.1:9092 --list`
- you can monitor messages with `docker exec -i kafka-websocket-proxy_kafka_1 kafka-console-consumer --bootstrap-server 127.1:9092 --topic topicname`
- also you can produce message to topic `echo -n '{"ok": 1}' | docker exec -i kafka-websocket-proxy_kafka_1 kafka-console-producer --bootstrap-server 127.1:9092 --topic topicname --sync`
- or many messages `for i in $(seq 2); do echo -n "{\"numbers\": ${i}}" | docker exec -i kafka-websocket-proxy_kafka_1 kafka-console-producer --bootstrap-server 127.1:9092 --topic topicname --sync; done;`
- run application with `go run -v cmd/kafka-websocket-proxy/*.go`
- connect client to it `websocat ws://127.0.0.1:8080/ws/name/topicname/`
