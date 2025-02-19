import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaStorage {
    private static final ConcurrentHashMap<String, List<byte[]>> topicLogs = new ConcurrentHashMap<>();

    public static void storeMessage(String topic, byte[] message){
        topicLogs.computeIfAbsent(topic, k -> new ArrayList<>()).add(message);
    }

    public static List<byte[]> fetchMessages(String topic){
        return topicLogs.getOrDefault(topic, new ArrayList<>());
    }

    public static List<String> getTopics(){
        return new ArrayList<>(topicLogs.keySet());
    }
}
