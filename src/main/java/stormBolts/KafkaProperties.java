package stormBolts;

public interface KafkaProperties {
    final static String zkConnect ="Node1:2181,Node2:2181,Node3:2181";
    final static String brokers ="Node1:9092,Node2:9092,Node3:9092";
    final static String topic = "TP_ZJ32";
    final static String groupId = "group1";
}
