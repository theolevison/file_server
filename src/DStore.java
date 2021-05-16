import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;

public class DStore {

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder= args[3];
        File store = new File(file_folder);
        if (!store.exists()){
            store.mkdir();
        }

        //start server and keep checking for connections
        try {
            ServerSocket clientSocket = new ServerSocket(port);

            Socket controller = new Socket(InetAddress.getLocalHost() ,cport);
            OutputStream controllerOut = controller.getOutputStream();
            System.out.println("dstore: connecting to controller");
            controllerOut.write(("JOIN "+port).getBytes(StandardCharsets.UTF_8));

            InputStream controllerIn = controller.getInputStream();
            //TODO: check if connection to controller is successful

            for (; ; ) {
                try {
                    System.out.println("dstore: waiting for connection");
                    Socket client = clientSocket.accept();
                    System.out.println("dstore: connected");
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


                            controllerOut.write(("STORE_ACK " + fileName).getBytes(StandardCharsets.UTF_8));

                        } catch (IOException ioException) {
                            ioException.printStackTrace();
                        }
                    } else if (command.equals("LOAD_DATA")) {
                        //get file name
                        int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                        String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
                        System.out.println("fileName " + fileName);

                        try {
                            File file = new File(fileName);

                            if (file.exists()) {
                                FileInputStream inf = new FileInputStream(file);
                                OutputStream clientOut = client.getOutputStream();

                                while ((buflen = inf.read(buf)) != -1) {
                                    System.out.println("*");
                                    clientOut.write(buf, 0, buflen);
                                }
                                inf.close();
                                clientOut.close();
                            }
                            clientIn.close();
                            client.close();

                        } catch (Exception e) {
                            System.out.println(e);
                        }

                    } else if (command.equals("REMOVE")) {
                        //get file name
                        int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                        String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
                        System.out.println("fileName " + fileName);

                        try {
                            File file = new File(fileName);
                            if (file.exists()) {
                                if (file.delete()) {
                                    controllerOut.write(("REMOVE_ACK " + fileName).getBytes(StandardCharsets.UTF_8));
                                } else {
                                    System.out.println("unable to delete file " + fileName);
                                }
                            } else {
                                controllerOut.write(("ERROR_FILE_DOES_NOT_EXIST " + fileName).getBytes(StandardCharsets.UTF_8));
                            }

                        } catch (Exception e) {
                            System.out.println(e);
                        }
                    } else if (command.equals("REBALANCE")) {
                        //find dstores to send stuff to
                        int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                        int numberToSend = Integer.parseInt(firstBuffer.substring(firstSpace + 1, secondSpace));
                        System.out.println("number of files to send " + numberToSend);

                        String buffer = firstBuffer.substring(secondSpace + 1, buflen);
                        for (int i = 0; i < numberToSend; i++) {

                            firstSpace = buffer.indexOf(" ", 0);
                            String fileToSend = buffer.substring(0, firstSpace);
                            System.out.println("file to send " + fileToSend);

                            secondSpace = buffer.indexOf(" ", secondSpace + 1);
                            int numberOfDstores = Integer.parseInt(buffer.substring(firstSpace + 1, secondSpace));
                            buffer = firstBuffer.substring(secondSpace + 1, buflen);
                            for (int j = 0; j < numberOfDstores; j++) {
                                firstSpace = buffer.indexOf(" ", 0);
                                String dstore = buffer.substring(0, firstSpace);
                                buffer = firstBuffer.substring(firstSpace + 1, buflen);
                                System.out.println("dstore that needs a file on port " + dstore);

                                try {
                                    InputStream dstoreIn = new ServerSocket(Integer.parseInt(dstore)).accept().getInputStream();
                                    File file = new File(fileToSend);

                                    OutputStream dstoreOut = new ServerSocket(Integer.parseInt(dstore)).accept().getOutputStream();

                                    if (file.exists()) {
                                        dstoreOut.write(("REBALANCE_STORE " + fileToSend + file.length()).getBytes(StandardCharsets.UTF_8));

                                        //find the command
                                        buflen = clientIn.read(buf);
                                        firstBuffer = new String(buf, 0, buflen);
                                        firstSpace = firstBuffer.indexOf(" ");
                                        command = firstBuffer.substring(0, firstSpace);
                                        System.out.println("command " + command);

                                        if (command.equals("ACK")) {
                                            FileInputStream inf = new FileInputStream(file);

                                            while ((buflen = inf.read(buf)) != -1) {
                                                System.out.println("*");
                                                dstoreOut.write(buf, 0, buflen);
                                            }
                                            inf.close();
                                        } else {
                                            //TODO: log error no ACK
                                        }
                                        dstoreOut.close();
                                    }
                                    dstoreIn.close();
                                } catch (Exception e) {
                                    System.out.println(e);
                                }
                            }

                        }

                        //find what to delete
                        firstSpace = firstBuffer.indexOf(" ", 0);
                        int numberToRemove = Integer.parseInt(firstBuffer.substring(firstSpace + 1, secondSpace));
                        buffer = firstBuffer.substring(firstSpace + 1, buflen);
                        for (int i = 0; i < numberToRemove; i++) {
                            firstSpace = buffer.indexOf(" ", 0);
                            String file = buffer.substring(0, firstSpace);
                            buffer = firstBuffer.substring(firstSpace + 1, buflen);

                            //remove file from server
                            File file2 = new File(file);
                            file2.delete();
                        }


                    } else if (command.equals("REBALANCE_STORE")) {
                        //get file name
                        int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                        String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
                        System.out.println("fileName " + fileName);

                        //get file size
                        int filesize = Integer.parseInt(firstBuffer.substring(secondSpace + 1, buflen));

                        //here the client is actually a dstore
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