public class KafkaRequest {
    short apiKey;
    short apiVersion;
    int correlationId;
    byte[] body;

    public KafkaRequest(short apiKey, short apiVersion, int correlationId, byte[] body){
        this.apiKey = apiKey;
        this.apiVersion = apiVersion;
        this.correlationId = correlationId;
        this.body = body;
    }
}
