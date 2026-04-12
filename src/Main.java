import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class Main {
    public static void main(String[] args) {
        System.out.println("Server started on port 6379...");

        int port = 6379;
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                executorService.execute(() -> handleClient(clientSocket));
            }

        } catch (IOException e) {
            System.out.println("IOException: " + e.getMessage());
        }
    }
    public static void handleClient(Socket client) {

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
