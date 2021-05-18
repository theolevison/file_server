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
            System.out.println("connecting to controller");
            ServerSocket clientSocket = new ServerSocket(port);
            Socket controller = new Socket(InetAddress.getLocalHost() ,cport);
            OutputStream controllerOutputStream = controller.getOutputStream();
            PrintWriter controllerOut = new PrintWriter(controllerOutputStream);
            //controller.getOutputStream().write(("JOIN "+port).getBytes(StandardCharsets.UTF_8));
            controllerOut.println("JOIN "+port);
            controllerOut.println("JOIN "+port);
            controllerOut.println("JOIN "+port);
            System.out.println("connection successful");

            InputStream controllerIn = controller.getInputStream();
            //TODO: check if connection to controller is successful

            for (; ; ) {
                try {
                    System.out.println("waiting for connection");
                    Socket client = clientSocket.accept();

                    new Thread(new Runnable(){
                        public void run() {
                            System.out.println("new thread");

                            try {
                                //PrintWriter clientOut = new PrintWriter(client.getOutputStream());
                                InputStream clientInputStream = client.getInputStream();
                                BufferedReader clientIn = new BufferedReader(new InputStreamReader(clientInputStream));
                                OutputStream clientOutputStream = client.getOutputStream();
                                PrintWriter clientOut = new PrintWriter(clientOutputStream);

                                byte[] buf = new byte[1000];
                                int buflen;

                                try {
                                    System.out.println("checking for input");
                                    String input;

                                    while ((input = clientIn.readLine()) != null) {

                                        //find the command
                                        int firstSpace = input.indexOf(" ");
                                        String command = input.substring(0, firstSpace);
                                        System.out.println("command " + command);

                                        if (command.equals("STORE")) {
                                            //get file name
                                            int secondSpace = input.indexOf(" ", firstSpace + 1);
                                            String fileName = input.substring(firstSpace + 1, secondSpace);
                                            System.out.println("fileName " + fileName);

                                            //get file size
                                            int filesize = Integer.parseInt(input.substring(secondSpace + 1));

                                            try {
                                                clientOut.println("ACK");
                                                System.out.println("sent ack");
                                                //clientInputStream = client.getInputStream();


                                                //find the file content and write it to file
                                                //File outputFile = new File(fileName);
                                                System.out.println("test1 " + filesize);
                                                //FileWriter out2 = new FileWriter(outputFile);
                                                FileOutputStream fileout = new FileOutputStream(file_folder + "/" + fileName);
                                                System.out.println("test2");
                                                BufferedOutputStream out = new BufferedOutputStream(fileout);
                                                //clientInputStream.readNBytes(buf,0,filesize);
                                                byte [] buf2 = new byte[filesize];
                                                buflen = clientInputStream.readNBytes(buf2, 0, buf2.length-1);
                                                System.out.println("test3 " + buflen);
                                                fileout.write(buf2);
                                                //out2.write(buf);

                                                System.out.println("test3");
                                                out.close();
                                                fileout.close();
                                                clientInputStream.close();

                                                controllerOut.println("STORE_ACK " + fileName);

                                            } catch (IOException ioException) {
                                                ioException.printStackTrace();
                                            }
                                        } else if (command.equals("LOAD_DATA")) {
                                            //get file name
                                            int secondSpace = input.indexOf(" ", firstSpace + 1);
                                            String fileName = input.substring(firstSpace + 1, secondSpace);
                                            System.out.println("fileName " + fileName);

                                            try {
                                                File file = new File(fileName);

                                                if (file.exists()) {
                                                    FileInputStream inf = new FileInputStream(file);

                                                    while ((buflen = inf.read(buf)) != -1) {
                                                        System.out.println("*");
                                                        clientOutputStream.write(buf, 0, buflen);
                                                    }
                                                    inf.close();
                                                }

                                            } catch (Exception e) {
                                                System.out.println(e);
                                            }

                                        } else if (command.equals("REMOVE")) {
                                            //get file name
                                            int secondSpace = input.indexOf(" ", firstSpace + 1);
                                            String fileName = input.substring(firstSpace + 1, secondSpace);
                                            System.out.println("fileName " + fileName);

                                            try {
                                                File file = new File(fileName);
                                                if (file.exists()) {
                                                    if (file.delete()) {
                                                        controllerOut.println("REMOVE_ACK " + fileName);
                                                    } else {
                                                        System.out.println("unable to delete file " + fileName);
                                                    }
                                                } else {
                                                    controllerOut.println("ERROR_FILE_DOES_NOT_EXIST " + fileName);
                                                }

                                            } catch (Exception e) {
                                                System.out.println(e);
                                            }
                                        } else if (command.equals("REBALANCE")) {
                                            //find dstores to send stuff to
                                            int secondSpace = input.indexOf(" ", firstSpace + 1);
                                            int numberToSend = Integer.parseInt(input.substring(firstSpace + 1, secondSpace));
                                            System.out.println("number of files to send " + numberToSend);

                                            String buffer = input.substring(secondSpace + 1);
                                            for (int i = 0; i < numberToSend; i++) {

                                                firstSpace = buffer.indexOf(" ");
                                                String fileToSend = buffer.substring(0, firstSpace);
                                                System.out.println("file to send " + fileToSend);

                                                secondSpace = buffer.indexOf(" ", secondSpace + 1);
                                                int numberOfDstores = Integer.parseInt(buffer.substring(firstSpace + 1, secondSpace));
                                                buffer = input.substring(secondSpace + 1);
                                                for (int j = 0; j < numberOfDstores; j++) {
                                                    firstSpace = buffer.indexOf(" ", 0);
                                                    String dstore = buffer.substring(0, firstSpace);
                                                    buffer = input.substring(firstSpace + 1);
                                                    System.out.println("dstore that needs a file on port " + dstore);

                                                    try {
                                                        InputStream dstoreIn = new ServerSocket(Integer.parseInt(dstore)).accept().getInputStream();
                                                        File file = new File(fileToSend);

                                                        OutputStream dstoreOut = new ServerSocket(Integer.parseInt(dstore)).accept().getOutputStream();

                                                        if (file.exists()) {
                                                            dstoreOut.write(("REBALANCE_STORE " + fileToSend + file.length()).getBytes(StandardCharsets.UTF_8));

                                                            //find the command
                                                            buflen = clientInputStream.read(buf);
                                                            input = new String(buf, 0, buflen);
                                                            firstSpace = input.indexOf(" ");
                                                            command = input.substring(0, firstSpace);
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
                                            firstSpace = input.indexOf(" ", 0);
                                            int numberToRemove = Integer.parseInt(input.substring(firstSpace + 1, secondSpace));
                                            buffer = input.substring(firstSpace + 1);
                                            for (int i = 0; i < numberToRemove; i++) {
                                                firstSpace = buffer.indexOf(" ", 0);
                                                String file = buffer.substring(0, firstSpace);
                                                buffer = input.substring(firstSpace + 1);

                                                //remove file from server
                                                File file2 = new File(file);
                                                file2.delete();
                                            }


                                        } else if (command.equals("REBALANCE_STORE")) {
                                            //get file name
                                            int secondSpace = input.indexOf(" ", firstSpace + 1);
                                            String fileName = input.substring(firstSpace + 1, secondSpace);
                                            System.out.println("fileName " + fileName);

                                            //get file size
                                            int filesize = Integer.parseInt(input.substring(secondSpace + 1));

                                            //here the client is actually a dstore
                                            try {
                                                clientOut.println("ACK");

                                                //find the file content and write it to file
                                                File outputFile = new File(fileName);
                                                FileOutputStream fileout = new FileOutputStream(outputFile);
                                                fileout.write(input.getBytes(StandardCharsets.UTF_8), 0, input.length());
                                                while ((buflen = clientInputStream.read(buf)) != -1) {
                                                    System.out.print("*");
                                                    fileout.write(buf, 0, buflen);
                                                }
                                                fileout.close();

                                            } catch (IOException ioException) {
                                                ioException.printStackTrace();
                                            }
                                        } else {
                                            //malformed command
                                            //TODO: log error and continue
                                        }
                                    }
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }).start();
                } catch (Exception e) {
                    System.out.println(e);
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}