import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Main {
  static Map<String, String> store = new HashMap<>();
  static Map<String, Long> expiryMap = new HashMap<>();
  static Map<String, List<String>> listStore = new HashMap<>();
  static Map<String, Object> listlocks = new ConcurrentHashMap<>();

  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    System.out.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    ServerSocket serverSocket = null;
    int port = 6379;

    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      while (true) {
        // Read data from the client.
        Socket clientSocket = serverSocket.accept();
        new Thread(() -> handleClient(clientSocket)).start();
      }

    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }

  static void handleClient(Socket clientSocket) {
    try (clientSocket;
        OutputStream outputStream = clientSocket.getOutputStream();
        BufferedReader in = new BufferedReader(
            new InputStreamReader(clientSocket.getInputStream()))) {
      // Send a simple response to the client
      while (true) {
        String line = in.readLine();
        if (line == null)
          break;
        if (line.startsWith("*")) {
          int numArgs = Integer.parseInt(line.substring(1));
          String[] args = new String[numArgs];
          for (int i = 0; i < numArgs; i++) {
            in.readLine(); // skip $length
            args[i] = in.readLine();
          }
          if (args[0].equalsIgnoreCase("PING")) {
            outputStream.write("+PONG\r\n".getBytes());
            continue;
          }
          if (args[0].equalsIgnoreCase("ECHO")) {
            outputStream.write(("+" + args[1] + "\r\n").getBytes());
            continue;
          }
          if (args[0].equalsIgnoreCase("SET")) {
            store.put(args[1], args[2]);
            // Handle expiry if present
            if (numArgs >= 5 && args[3].equalsIgnoreCase("px")) {
              long seconds = Long.parseLong(args[4]);
              long expiry = System.currentTimeMillis() + seconds;
              expiryMap.put(args[1], expiry);
            } else {
              expiryMap.remove(args[1]);
            }
            outputStream.write("+OK\r\n".getBytes());
            continue;
          }
          if (args[0].equalsIgnoreCase("GET")) {
            String value = store.get(args[1]);
            Long expiry = expiryMap.get(args[1]);
            if (expiry != null && System.currentTimeMillis() > expiry) {
              store.remove(args[1]);
              expiryMap.remove(args[1]);
              outputStream.write("$-1\r\n".getBytes());
              continue;
            }
            if (value != null) {
              outputStream.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
            } else {
              outputStream.write("$-1\r\n".getBytes());
            }
            continue;
          }
          if (args[0].equalsIgnoreCase("RPUSH")) {
            String key = args[1];
            // String value = args[2];
            listStore.putIfAbsent(key, new ArrayList<>());
            List<String> list = listStore.get(key);
            for (int i = 2; i < args.length; i++) {
              String value = args[i];
              list.add(value);
            }
            Object lock = listlocks.get(key);
            if (lock != null) {
              synchronized (lock) {
                lock.notifyAll();
              }
            }

            outputStream.write((":" + list.size() + "\r\n").getBytes());
            continue;

          }
          if (args[0].equalsIgnoreCase("LRANGE")) {
            String key = args[1];
            int start = Integer.parseInt(args[2]);
            int end = Integer.parseInt(args[3]);
            List<String> list = listStore.getOrDefault(key, new ArrayList<>());
            if (start < 0)
              start += list.size();
            if (end < 0)
              end += list.size();
            if (start < 0)
              start = 0;
            if (end >= list.size())
              end = list.size() - 1;
            if (start > end || start >= list.size()) {
              outputStream.write("*0\r\n".getBytes());
              continue;
            }
            StringBuilder response = new StringBuilder("*" + (end - start + 1) + "\r\n");
            for (int i = start; i <= end; i++) {
              response.append("$").append(list.get(i).length()).append("\r\n")
                  .append(list.get(i)).append("\r\n");
            }
            outputStream.write(response.toString().getBytes());
            continue;
          }
          if (args[0].equalsIgnoreCase("LPUSH")) {
            String key = args[1];
            listStore.putIfAbsent(key, new ArrayList<>());
            List<String> list = listStore.get(key);
            for (int i = 2; i < args.length; i++) {
              String value = args[i];
              list.add(0, value);
            }
            Object lock = listlocks.get(key);
            if (lock != null) {
              synchronized (lock) {
                lock.notifyAll();
              }
            }
            outputStream.write((":" + list.size() + "\r\n").getBytes());
            continue;
          }
          if (args[0].equalsIgnoreCase("LLEN")) {
            String key = args[1];
            List<String> list = listStore.getOrDefault(key, new ArrayList<>());
            outputStream.write((":" + list.size() + "\r\n").getBytes());
            continue;
          }
          if (args[0].equalsIgnoreCase("LPOP")) {
            String key = args[1];
            List<String> list = listStore.getOrDefault(key, new ArrayList<>());
            if (list.isEmpty()) {
              outputStream.write("$-1\r\n".getBytes());
            } else {
              if (args.length < 3) {
                String value = list.remove(0);
                outputStream.write(("$" + value.length() + "\r\n" + value + "\r\n").getBytes());
              } else {
                // If a range is specified, return the first 'range' elements
                int range = Integer.parseInt(args[2]);

                StringBuilder response = new StringBuilder("*" + Math.min(range, list.size()) + "\r\n");
                while (range-- > 0 && !list.isEmpty()) {
                  String value = list.remove(0);
                  response.append("$").append(value.length()).append("\r\n")
                      .append(value).append("\r\n");
                }
                outputStream.write(response.toString().getBytes());
                if (list.isEmpty()) {
                  listStore.remove(key);
                } else {
                  listStore.put(key, list);
                }
              }
              continue;
            }
          }
          if (args[0].equalsIgnoreCase("BLPOP")) {
            String key = args[1];
            listStore.putIfAbsent(key, new ArrayList<>());
            listlocks.putIfAbsent(key, new Object());
            List<String> list = listStore.get(key);
            synchronized (listlocks.get(key)) {
              while (list.isEmpty()) {
                try {
                  if (args[2].equalsIgnoreCase("0")) {
                    listlocks.get(key).wait(); // Wait indefinitely
                  } else {
                    int timeout = Integer.parseInt(args[2]);
                    if (timeout < 0) {
                      outputStream.write("$-1\r\n".getBytes());
                      return;
                    }
                    listlocks.get(key).wait(timeout * 1000);

                  }
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  outputStream.write("$-1\r\n".getBytes());
                  return;
                }
              }
            }

            if (!list.isEmpty()) {
              String value = list.remove(0);
              outputStream.write(("*2" + "\r\n" + "$" + key.length() + "\r\n" + key + "\r\n"
                  + "$" + value.length() + "\r\n" + value + "\r\n").getBytes());
              if (list.isEmpty()) {
                listStore.remove(key);
              }
            } else {
              outputStream.write("$-1\r\n".getBytes());
            }

            continue;
          }
        }
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}
