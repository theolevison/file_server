import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class DStore {

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder= args[3];

        //start server and keep checking for connections
        try {
            ServerSocket clientSocket = new ServerSocket(port);
            ServerSocket controllerSocket = new ServerSocket(cport);

            Socket controller = controllerSocket.accept();
            OutputStream controllerOut = controller.getOutputStream();
            InputStream controllerIn = controller.getInputStream();
            //TODO: check if connection to controller is successful

            for (; ; ) {
                try {
                    System.out.println("waiting for connection");
                    Socket client = clientSocket.accept();
                    System.out.println("connected");
                    InputStream clientIn = client.getInputStream();

                    byte[] buf = new byte[1000];
                    int buflen;

                    //find the command
                    buflen = clientIn.read(buf);
                    String firstBuffer = new String(buf, 0, buflen);
                    int firstSpace = firstBuffer.indexOf(" ");
                    String command = firstBuffer.substring(0, firstSpace);
                    System.out.println("command " + command);

                    if (command.equals("STORE")) {
                        //get file name
                        int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                        String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
                        System.out.println("fileName " + fileName);

                        //get file size
                        int filesize = Integer.parseInt(firstBuffer.substring(secondSpace + 1, buflen));

                        //TODO: ack
                        try {
                            OutputStream clientOut = client.getOutputStream();
                            clientOut.write("ACK".getBytes(StandardCharsets.UTF_8));

                            //find the file content and write it to file
                            File outputFile = new File(fileName);
                            FileOutputStream fileout = new FileOutputStream(outputFile);
                            fileout.write(buf, 0, buflen);
                            while ((buflen = clientIn.read(buf)) != -1) {
                                System.out.print("*");
                                fileout.write(buf, 0, buflen);
                            }
                            fileout.close();
                            clientIn.close();
                            clientOut.close();
                            client.close();


                            controllerOut.write(("STORE_ACK "+fileName).getBytes(StandardCharsets.UTF_8));

                        } catch (IOException ioException) {
                            ioException.printStackTrace();
                        }
                    } else {
                        //malformed command
                        //TODO: log error and continue
                    }
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}