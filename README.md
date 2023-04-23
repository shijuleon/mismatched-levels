# mismatched-levels

Using Kafka with multiple types of consumers instead of demultiplexing and requeueing in another topic. Allows consumers to consume at their own pace by using consumer groups.

Slow consumer randomly fails and may need multiple runs to consume all intended messages.

```
cd docker && docker compose up -d
```

```
docker exec broker kafka-topics --bootstrap-server broker:9092 \
             --create \
             --topic mismatched-levels
```
