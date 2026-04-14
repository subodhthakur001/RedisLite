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

    static ConcurrentHashMap<String, ValueWithExpiry> kvStore = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, List<String>> listMap = new ConcurrentHashMap<>();
    static ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        System.out.println("Server started on port 6379...");

        ExecutorService executor = Executors.newFixedThreadPool(10);

        try (ServerSocket serverSocket = new ServerSocket(6379)) {
            while (true) {
                Socket client = serverSocket.accept();
                executor.execute(() -> handleClient(client));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void handleClient(Socket client) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                PrintWriter out = new PrintWriter(client.getOutputStream(), true)
        ) {

            String line;

            while ((line = in.readLine()) != null) {

                if (!line.startsWith("*")) break;

                int n = Integer.parseInt(line.substring(1));
                String[] args = new String[n];

                for (int i = 0; i < n; i++) {
                    in.readLine(); // skip $len
                    args[i] = in.readLine();
                }

                String cmd = args[0].toUpperCase();

                switch (cmd) {

                    case "PING":
                        out.print("+PONG\r\n");
                        break;

                    case "ECHO":
                        out.print(createBulkString(args[1]));
                        break;

                    case "SET": {
                        String key = args[1];
                        String val = args[2];

                        long expiry = -1;

                        if (args.length >= 5 && args[3].equalsIgnoreCase("PX")) {
                            long px = Long.parseLong(args[4]);
                            expiry = System.currentTimeMillis() + px;
                        }

                        kvStore.put(key, new ValueWithExpiry(val, expiry));
                        out.print("+OK\r\n");
                        break;
                    }

                    case "GET": {
                        String key = args[1];
                        ValueWithExpiry v = kvStore.get(key);

                        if (v == null) {
                            out.print("$-1\r\n");
                            break;
                        }

                        if (v.expiryTime != -1 && System.currentTimeMillis() > v.expiryTime) {
                            kvStore.remove(key);
                            out.print("$-1\r\n");
                            break;
                        }

                        out.print(createBulkString(v.value));
                        break;
                    }

                    case "RPUSH": {
                        String key = args[1];
                        Object lock = locks.computeIfAbsent(key, k -> new Object());

                        synchronized (lock) {
                            List<String> list = listMap.computeIfAbsent(key, k -> new ArrayList<>());

                            for (int i = 2; i < args.length; i++) {
                                list.add(args[i]);
                            }

                            lock.notifyAll();
                            out.print(":" + list.size() + "\r\n");
                        }
                        break;
                    }

                    case "LPUSH": {
                        String key = args[1];
                        Object lock = locks.computeIfAbsent(key, k -> new Object());

                        synchronized (lock) {
                            List<String> list = listMap.computeIfAbsent(key, k -> new ArrayList<>());

                            for (int i = 2; i < args.length; i++) {
                                list.add(0, args[i]); // insert at front
                            }

                            lock.notifyAll();
                            out.print(":" + list.size() + "\r\n");
                        }
                        break;
                    }
                    case "LLEN":
                        if(args.length >1){
                            String key = args[1];
                            List<String> existingList = listMap.get(key);
                            if(existingList != null){
                                out.print(":" + listMap.get(key).size() + "\r\n");
                            }
                            else{
                                out.print(":0" + "\r\n");
                            }
                        }
                        break;
                    case "LRANGE": {
                        String key = args[1];
                        int start = Integer.parseInt(args[2]);
                        int stop = Integer.parseInt(args[3]);

                        List<String> list = listMap.get(key);
                        if (list == null) {
                            out.print("*0\r\n");
                            break;
                        }

                        int size = list.size();

                        if (start < 0) start += size;
                        if (stop < 0) stop += size;

                        start = Math.max(0, start);
                        stop = Math.min(size - 1, stop);

                        if (start > stop) {
                            out.print("*0\r\n");
                            break;
                        }

                        StringBuilder res = new StringBuilder();
                        res.append("*").append(stop - start + 1).append("\r\n");

                        for (int i = start; i <= stop; i++) {
                            res.append(createBulkString(list.get(i)));
                        }

                        out.print(res.toString());
                        break;
                    }

                    case "LPOP":
                        if (args.length >= 2) {
                            String key = args[1];
                            List<String> list = listMap.get(key);

                            if (args.length == 3) {
                                int count = Integer.parseInt(args[2]);

                                if (list == null || list.isEmpty()) {
                                    out.print("*0\r\n");
                                    break;
                                }

                                int actual = Math.min(count, list.size());

                                StringBuilder res = new StringBuilder();
                                res.append("*").append(actual).append("\r\n");

                                for (int i = 0; i < actual; i++) {
                                    String val = list.remove(0);
                                    res.append("$")
                                            .append(val.length())
                                            .append("\r\n")
                                            .append(val)
                                            .append("\r\n");
                                }

                                out.print(res.toString());
                            }

                            else {
                                if (list == null || list.isEmpty()) {
                                    out.print("$-1\r\n");
                                } else {
                                    out.print(createBulkString(list.remove(0)));
                                }
                            }

                        } else {
                            out.print("-ERR wrong number of arguments for 'lpop'\r\n");
                        }
                        break;

                    case "BLPOP": {
                        String key = args[1];
                        int timeout = Integer.parseInt(args[2]);

                        Object lock = locks.computeIfAbsent(key, k -> new Object());

                        synchronized (lock) {
                            long end = timeout == 0 ? Long.MAX_VALUE :
                                    System.currentTimeMillis() + timeout * 1000L;

                            while (true) {
                                List<String> list = listMap.get(key);

                                if (list != null && !list.isEmpty()) {
                                    String val = list.remove(0);
                                    out.print(createArrayResponse(key, val));
                                    break;
                                }

                                long remaining = end - System.currentTimeMillis();
                                if (remaining <= 0) {
                                    out.print("*-1\r\n");
                                    break;
                                }

                                lock.wait(remaining);
                            }
                        }
                        break;
                    }

                    default:
                        out.print("-ERR unknown command\r\n");
                }

                out.flush();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String createBulkString(String s) {
        return "$" + s.length() + "\r\n" + s + "\r\n";
    }

    private static String createArrayResponse(String key, String val) {
        return "*2\r\n" +
                "$" + key.length() + "\r\n" + key + "\r\n" +
                "$" + val.length() + "\r\n" + val + "\r\n";
    }
}