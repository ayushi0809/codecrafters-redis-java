import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.io.OutputStream;
import java.net.Socket;

public class Main {
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
        String inputLine = in.readLine();
        if (inputLine == null) {
          break;
        }
        System.out.println("Received: " + inputLine);
        // Parse RESP array for ECHO command
        if (inputLine.startsWith("*2")) { // Array of 2 elements
          String cmd = in.readLine(); // $4
          String echoCmd = in.readLine(); // ECHO
          String argLen = in.readLine(); // $3 (or other length)
          String arg = in.readLine(); // hey (or other argument)
          System.out.println("Command: " + echoCmd + ", Arg: " + arg);
          System.out.println("Arg Length: " + argLen);
          if (echoCmd.equalsIgnoreCase("ECHO")) {
            outputStream.write((argLen + "\r\n" + arg + "\r\n").getBytes());
            System.out.println("Wrote echo response: " + arg);
            continue;
          }
        }
        // Fallback for PING
        if (inputLine.equalsIgnoreCase("PING")) {
          outputStream.write("+PONG\r\n".getBytes());
          System.out.println("Wrote ping");
        }
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    }
  }
}
