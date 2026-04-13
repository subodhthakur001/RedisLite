import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Main {
    public static void main(String[] args) {
        System.out.println("Server started on port 6379...");

        int port = 6379;
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        ConcurrentHashMap<String,ValueWithExpiry> KeyVsValueHashmap = new ConcurrentHashMap<>();
        HashMap<String, List<String>> listMap = new HashMap<>();

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                executorService.execute(() -> handleClient(clientSocket, KeyVsValueHashmap,listMap));
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
    public static void handleClient(Socket client, 
                                   ConcurrentHashMap<String,ValueWithExpiry> KeyVsValueHashmap,
                                   HashMap<String, List<String>> listMap) {

        try (
                Socket s = client;
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
                PrintWriter out = new PrintWriter(s.getOutputStream(), true)
        ) {
            String message;

            while ((message = in.readLine()) != null) {

                // RESP array starts with *
                if (message.startsWith("*")) {

                    int numArgs = Integer.parseInt(message.substring(1));
                    String[] args = new String[numArgs];

                    // Read all arguments
                    for (int i = 0; i < numArgs; i++) {
                        in.readLine();           // skip $length
                        args[i] = in.readLine(); // actual value
                    }

                    // Debug (optional)
                    // System.out.println("Received: " + Arrays.toString(args));

                    String command = args[0].toUpperCase();

                    switch (command) {
                        case "PING":
                            out.print("+PONG\r\n");
                            break;

                        case "ECHO":
                            if (args.length > 1) {
                                out.print(createBulkString(args[1]));
                            } else {
                                out.print("-ERR wrong number of arguments for 'echo'\r\n");
                            }
                            break;
                        case "SET":
                            if(args.length > 1){
                                String key = args[1];
                                String value = args[2];

                                long ExpiryTime = -1;
                                if(args.length >= 5 && args[3].equalsIgnoreCase("PX")){
                                    long px = Long.parseLong(args[4]);
                                    ExpiryTime = System.currentTimeMillis() + px;
                                }
                                KeyVsValueHashmap.put(key, new ValueWithExpiry(value, ExpiryTime));
                                out.print("+OK\r\n");
                            }
                            break;
                        case "GET":
                            if (args.length >= 2) {
                                String key = args[1];

                                ValueWithExpiry entry = KeyVsValueHashmap.get(key);

                                if (entry == null) {
                                    out.print("$-1\r\n");
                                    break;
                                }

                                if (entry.expiryTime != -1 && System.currentTimeMillis() > entry.expiryTime) {
                                    KeyVsValueHashmap.remove(key);
                                    break;
                                }

                                out.print(createBulkString(entry.value));
                            } else {
                                out.print("-ERR wrong number of arguments for 'get'\r\n");
                            }
                            break;
                        case "RPUSH":
                            if(args.length >= 3){
                                String listName = args[1];
                                String value = args[2];
                                if(listMap.containsKey(listName)){
                                    List<String> existingList = listMap.get(listName);
                                    existingList.add(value);
                                    out.print(":" + listMap.get(listName).size() + "\r\n");
                                }
                                else {
                                    List<String> list = new ArrayList<>();
                                    String newlistName = args[1];
                                    String element = args[2];
                                    list.add(element);
                                    listMap.put(newlistName, list);
                                    out.print(":" + listMap.get(newlistName).size() + "\r\n");
                                }
                            }
                            break;    
                        default:
                            out.print("-ERR unknown command\r\n");
                    }

                    out.flush();
                } else {
                    // Invalid protocol → close connection
                    break;
                }
            }

        } catch (IOException e) {
            System.out.println("Client disconnected: " + e.getMessage());
        }
    }


    private static String createBulkString(String response) {
        return "$" + response.length() + "\r\n" + response + "\r\n";
    }
}
