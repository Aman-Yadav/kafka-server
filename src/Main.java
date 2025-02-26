import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args){
        System.err.println("Logs will appear here!");


        int threadPoolSize = 10;
        int port = 9092;
        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);

        try (ServerSocket serverSocket = new ServerSocket(port)){

            // setting SO_REUSEADDR ensures that program doesn't run into 'Address already in use' errors
            serverSocket.setReuseAddress(true);
            System.out.println("Server listening on port :" + port);

            while(true){

                // Wait for connection from client.
                Socket clientSocket = serverSocket.accept();
                executorService.submit(() -> handleClient(clientSocket));
            }
        } catch (IOException e){
            e.printStackTrace();
        } finally {
            executorService.shutdown();
        }


    }

    private static void handleClient(Socket clientSocket){

        try{
            InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream();

            while(true){
                KafkaRequest request = parseRequest(inputStream);
                byte[] response = processRequest(request);
                sendResponse(outputStream, request.correlationId, response);

            }
        } catch (IOException e){
            System.out.println("Client Disconnected: " + e.getMessage());
        }

//        Using byte arrays directly here without parsing it into KafkaRequest

//        try {
//            while(true){
//
//                InputStream inputStream = clientSocket.getInputStream();
//                OutputStream outputStream = clientSocket.getOutputStream();
//                var response = new ByteArrayOutputStream();
//
//                KafkaRequest request = parseRequest(inputStream);
//
//                // Reading kafka request [order matters as format is predefined]
//                byte[] length = inputStream.readNBytes(4);
//                byte[] apiKey = inputStream.readNBytes(2);
//                byte[] apiVersion = inputStream.readNBytes(2);
//                byte[] correlationId = inputStream.readNBytes(4);
//
//                short shortApiVersion = ByteBuffer.wrap(apiVersion).getShort();
//                short shortApiKey = ByteBuffer.wrap(apiKey).getShort();
//
//                byte[] buffer = new byte[1024];
//                int bytesRead = inputStream.read(buffer);
//
//                response.write(correlationId);
//
//                if(shortApiVersion < 0 || shortApiVersion > 4){
//                    response.write(new byte[] {0, 35});
//                } else {
//                    if(shortApiKey == 18){
//                        response.write(new byte[] {0, 0});  // No error
//                        response.write(1);                   // Number of API keys described
//
//                        response.write(new byte[] {0, 18});  // API key for API_VERSIONS
//                        response.write(new byte[] {0, 3});   // Minimum version
//                        response.write(new byte[] {0, 4});   // Maximum version
////                      response.write(0);                   // Tagged fields
////                      response.write(new byte[] {0, 0, 0, 0}); // Throttle time
////                      response.write(0); // End of tagged fields
//
//
//                    } else if(shortApiKey == 1){
//                        response.write(new byte[] {0, 0});  // No error
//                        response.write(1);                   // Number of API keys described
//
//
//                        response.write(new byte[] {0, 1});  // API key for FETCH (1)
//                        response.write(new byte[] {0, 0});  // Minimum version for FETCH (0)
//                        response.write(new byte[] {0, 16}); // Maximum version for FETCH (16)
//                        response.write(0);                  // Tagged fields for FETCH
//                        response.write(new byte[] {0, 0, 0, 0}); // Throttle time for FETCH
//                        response.write(0);                  // End of tagged fields for FETCH
//                    }
//
//                }
//
//                int size = response.size();
//                byte[] sizeBytes = ByteBuffer.allocate(4).putInt(size).array();
//                var finalResponse = response.toByteArray();
//
//                System.out.println(Arrays.toString(sizeBytes));
//                System.out.println(Arrays.toString(finalResponse));
//
//                outputStream.write(sizeBytes);
//                outputStream.write(finalResponse);
//                outputStream.flush();
//            }
//
//        } catch (IOException e) {
//            System.out.println("IOException: " + e.getMessage());
//        } finally {
//            try {
//                if (clientSocket != null) {
//                    clientSocket.close();
//                }
//            } catch (IOException e) {
//                System.out.println("IOException: " + e.getMessage());
//            }
//        }
    }

    private static KafkaRequest parseRequest(InputStream inputStream) throws IOException{
        byte[] lengthBytes = inputStream.readNBytes(4);
        byte[] apiKeyBytes = inputStream.readNBytes(2);
        byte[] apiVersionBytes = inputStream.readNBytes(2);
        byte[] correlationIdBytes = inputStream.readNBytes(4);

        short apiKey = ByteBuffer.wrap(apiKeyBytes).getShort();
        short apiVersion = ByteBuffer.wrap(apiVersionBytes).getShort();
        int correlationId = ByteBuffer.wrap(correlationIdBytes).getInt();

        int bodyLength = ByteBuffer.wrap(lengthBytes).getInt() - 10; // excluding header size
        byte[] body = inputStream.readNBytes(bodyLength);

        return new KafkaRequest(apiKey, apiVersion, correlationId, body);

    }

    private static byte[] processRequest(KafkaRequest request) throws IOException {
        return switch (request.apiKey) {
            case KafkaApiKeys.PRODUCE -> handleProduce(request);
            case KafkaApiKeys.FETCH -> handleFetch(request);
            case KafkaApiKeys.METADATA -> handleMetaData();
            default -> createErrorResponse(request.correlationId);
        };
    }

    private static byte[] handleProduce(KafkaRequest request){
        String topic = new String(request.body, 0, 10).trim();
        byte[] message = Arrays.copyOfRange(request.body, 10, request.body.length);

        KafkaStorage.storeMessage(topic, message);
        // Send acknowledgment (Correlation ID + No error code)
        return new byte[] {0, 0};
    }

    private static byte[] handleFetch(KafkaRequest request) throws IOException {
        String topic = new String(request.body, 0, 10).trim();

        List<byte[]> messages = KafkaStorage.fetchMessages(topic);
        ByteArrayOutputStream response = new ByteArrayOutputStream();

        for(byte[] msg : messages){
            response.write(msg.length);
            response.write(msg);
        }

        return response.toByteArray();
    }

    private static byte[] handleMetaData() throws IOException {
        List<String> topics = KafkaStorage.getTopics();
        ByteArrayOutputStream response = new ByteArrayOutputStream();

        response.write(topics.size());
        for(String topic : topics){
            byte[] topicBytes = topic.getBytes();
            response.write(topicBytes.length);
            response.write(topicBytes);
        }

        return response.toByteArray();
    }

    public static void sendResponse(OutputStream outputStream, int correlationId, byte[] responseBody) throws IOException{
        ByteArrayOutputStream response = new ByteArrayOutputStream();

        response.write(ByteBuffer.allocate(4).putInt(responseBody.length + 4).array()); // length prefix
        response.write(ByteBuffer.allocate(4).putInt(correlationId).array()); // correlationID
        response.write(responseBody); // actual response data

        byte[] finalResponse = response.toByteArray();
        outputStream.write(finalResponse);
        outputStream.flush();
    }

    public static byte[] createErrorResponse(int correlationID) throws IOException {
        ByteArrayOutputStream response = new ByteArrayOutputStream();

        response.write(ByteBuffer.allocate(4).putInt(correlationID).array());
        response.write(new byte[] {0, 35}); // Error code 35 [unknown API]

        return response.toByteArray();
    }

}
