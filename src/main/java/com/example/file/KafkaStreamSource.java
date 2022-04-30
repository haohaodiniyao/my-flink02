package com.example.file;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Map;
import java.util.Properties;

public class KafkaStreamSource {
    public static FlinkKafkaConsumer<String> newStringFlinkKafkaConsumer(Map<String,String> configMap){
        Properties properties = new Properties();
        properties.putAll(configMap);
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(configMap.get(Constants.KAFKA_TOPIC), new SimpleStringSchema(), properties);
        return stringFlinkKafkaConsumer;
    }
}
