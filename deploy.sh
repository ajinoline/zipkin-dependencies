git checkout .
git pull
./mvnw -DskipTests clean install
STORAGE_TYPE=elasticsearch ES_HOSTS=192.168.32.149:9200  ES_DATE_SEPARATOR=- ES_NODES_WAN_ONLY=true java -jar ./main/target/zipkin-dependencies-1.11.2-SNAPSHOT.jar
