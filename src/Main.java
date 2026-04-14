import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
                                 List<String> list = listMap.computeIfAbsent(
                                    listName,
                                    k -> Collections.synchronizedList(new ArrayList<>())
                                    );
                                 for (int i = 2; i < args.length; i++) {
                                    list.add(args[i]);
                                    }
                                
                                 out.print(":" + list.size() + "\r\n");
                            } else {
                                out.print("-ERR wrong number of arguments for 'rpush'\r\n");
                            }
                            break;
                        case "LRANGE":
                            if (args.length >= 4) {
                                String listName = args[1];
                                int start = Integer.parseInt(args[2]);
                                int stop = Integer.parseInt(args[3]);

                                List<String> list = listMap.get(listName);

                                if (list == null) {
                                    out.print("*0\r\n");
                                    break;
                                }

                                int size = list.size();

                                if (start < 0) start = size + start;
                                if (stop < 0) stop = size + stop;

                                if (start < 0) start = 0;
                                if (stop >= size) stop = size - 1;

                                if (start > stop || start >= size) {
                                    out.print("*0\r\n");
                                    break;
                                }

                                StringBuilder response = new StringBuilder();
                                int count = stop - start + 1;

                                response.append("*").append(count).append("\r\n");

                                for (int i = start; i <= stop; i++) {
                                    String val = list.get(i);
                                    response.append("$")
                                            .append(val.length())
                                            .append("\r\n")
                                            .append(val)
                                            .append("\r\n");
                                }

                                out.print(response.toString());

                            } else {
                                out.print("-ERR wrong number of arguments for 'lrange'\r\n");
                            }
                            break;
                        case "LPUSH":
                            if(args.length >= 3){
                                String listname = args[1];
                                List<String> list = listMap.computeIfAbsent(
                                        listname,
                                        k -> Collections.synchronizedList(new ArrayList<>())
                                );
                                for (int i = 2; i < args.length; i++) {
                                    list.add(args[i]);
                                }
                                Collections.reverse(list);
                                out.print(":" + list.size() + "\r\n");
                            } else {
                                out.print("-ERR wrong number of arguments for 'rpush'\r\n");
                            }
                            break;
                        case "LLEN":
                            if(args.length >1){
                                String key = args[1];
//                                List<String> existingList = listMap.get(key);
                                List<String> existingList = listMap.get(key);
                                if(existingList != null){
                                    out.print(":" + listMap.get(key).size() + "\r\n");
                                }
                                else{
                                    out.print(":0" + "\r\n");
                                }
                            }
                            break;
                        case "LPOP":
                            if(args.length > 1){
                                String listName = args[1];
                                List<String> existingList = listMap.get(listName);
                                if(existingList != null) out.print(createBulkString(existingList.removeFirst()));
                                else out.print("$-1\r\n");
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
