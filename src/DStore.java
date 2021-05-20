import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;

public class DStore {

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder= args[3];

        File store = new File(file_folder);

        for (File file : store.listFiles()){
            file.delete();
        }
        store.delete();
        store = new File(file_folder);
        if (!store.exists()){
            if (!store.mkdir()){
                System.out.println("not able to create folder for dstore");
            }
        }
        DstoreLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL, port);

        //start server and keep checking for connections
        try {
            System.out.println("connecting to controller");
            ServerSocket clientSocket = new ServerSocket(port);
            Socket controller = new Socket(InetAddress.getLocalHost() ,cport);
            OutputStream controllerOutputStream = controller.getOutputStream();
            PrintWriter controllerOut = new PrintWriter(controllerOutputStream);
            //controller.getOutputStream().write(("JOIN "+port).getBytes(StandardCharsets.UTF_8));
            controllerOut.println(Protocol.JOIN_TOKEN + " " +port);
            controllerOut.flush();
            DstoreLogger.getInstance().messageSent(controller, Protocol.JOIN_TOKEN + " " +port);

            System.out.println("connection successful");

            InputStream controllerInputStream = controller.getInputStream();
            BufferedReader controllerIn = new BufferedReader(new InputStreamReader(controllerInputStream));

            //make a thread to handle instructions from the controller
            new Thread(new Runnable(){
                public void run() {
                    try {
                        String input;

                        while ((input = controllerIn.readLine()) != null) {
                            DstoreLogger.getInstance().messageReceived(controller, input);
                            //find the command
                            String[] commands = input.split(" ");
                            System.out.println("command " + Arrays.toString(commands));

                            if (commands[0].equals(Protocol.REMOVE_TOKEN)) {
                                //get file name
                                String fileName = commands[1];
                                System.out.println("fileName " + fileName);

                                try {
                                    File file = new File(file_folder + "/" + fileName);
                                    if (file.exists()) {
                                        if (file.delete()) {
                                            controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                                            controllerOut.flush();
                                            DstoreLogger.getInstance().messageSent(controller, Protocol.REMOVE_ACK_TOKEN + " " + fileName);
                                        } else {
                                            System.out.println("unable to delete file " + fileName);
                                        }
                                    } else {
                                        controllerOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
                                        controllerOut.flush();
                                        DstoreLogger.getInstance().messageSent(controller, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + fileName);
                                    }

                                } catch (Exception e) {
                                    System.err.println(e);
                                }
                            } else if (commands[0].equals("REBALANCE")) {
//                                //find dstores to send stuff to
//                                int numberToSend = Integer.parseInt(commands[2]);
//                                System.out.println("number of files to send " + numberToSend);
//
//                                String buffer = input.substring(secondSpace + 1);
//                                for (int i = 0; i < numberToSend; i++) {
//
//                                    firstSpace = buffer.indexOf(" ");
//                                    String fileToSend = buffer.substring(0, firstSpace);
//                                    System.out.println("file to send " + fileToSend);
//
//                                    secondSpace = buffer.indexOf(" ", secondSpace + 1);
//                                    int numberOfDstores = Integer.parseInt(buffer.substring(firstSpace + 1, secondSpace));
//                                    buffer = input.substring(secondSpace + 1);
//                                    for (int j = 0; j < numberOfDstores; j++) {
//                                        firstSpace = buffer.indexOf(" ", 0);
//                                        String dstore = buffer.substring(0, firstSpace);
//                                        buffer = input.substring(firstSpace + 1);
//                                        System.out.println("dstore that needs a file on port " + dstore);
//
//                                        try {
//                                            InputStream dstoreIn = new ServerSocket(Integer.parseInt(dstore)).accept().getInputStream();
//                                            File file = new File(fileToSend);
//
//                                            OutputStream dstoreOut = new ServerSocket(Integer.parseInt(dstore)).accept().getOutputStream();
//
//                                            if (file.exists()) {
//                                                dstoreOut.write(("REBALANCE_STORE " + fileToSend + file.length()).getBytes(StandardCharsets.UTF_8));
//
//                                                //find the command
//                                                buflen = clientInputStream.read(buf);
//                                                input = new String(buf, 0, buflen);
//                                                firstSpace = input.indexOf(" ");
//                                                commands[0] = input.substring(0, firstSpace);
//                                                System.out.println("command " + commands[0]);
//
//                                                if (commands[0].equals("ACK")) {
//                                                    FileInputStream inf = new FileInputStream(file);
//
//                                                    while ((buflen = inf.read(buf)) != -1) {
//                                                        System.out.println("*");
//                                                        dstoreOut.write(buf, 0, buflen);
//                                                    }
//                                                    inf.close();
//                                                } else {
//                                                    //TODO: log error no ACK
//                                                }
//                                                dstoreOut.close();
//                                            }
//                                            dstoreIn.close();
//                                        } catch (Exception e) {
//                                            System.out.println(e);
//                                        }
//                                    }
//
//                                }
//
//                                //find what to delete
//                                firstSpace = input.indexOf(" ", 0);
//                                int numberToRemove = Integer.parseInt(input.substring(firstSpace + 1, secondSpace));
//                                buffer = input.substring(firstSpace + 1);
//                                for (int i = 0; i < numberToRemove; i++) {
//                                    firstSpace = buffer.indexOf(" ", 0);
//                                    String file = buffer.substring(0, firstSpace);
//                                    buffer = input.substring(firstSpace + 1);
//
//                                    //remove file from server
//                                    File file2 = new File(file);
//                                    file2.delete();
//                                }
                            }
                        }
                    } catch (IOException e){
                        System.err.println(e);
                    }
                }
            }).start();
//
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

                                try {
                                    System.out.println("checking for input");
                                    String input;

                                    while ((input = clientIn.readLine()) != null) {
                                        DstoreLogger.getInstance().messageReceived(client, input);

                                        //find the command
                                        String[] commands = input.split(" ");

                                        switch (commands[0]) {
                                            case Protocol.STORE_TOKEN: {
                                                //get file name
                                                String fileName = commands[1];

                                                //get file size
                                                int filesize = Integer.parseInt(commands[2]);

                                                try {
                                                    clientOut.println(Protocol.ACK_TOKEN);
                                                    clientOut.flush();
                                                    DstoreLogger.getInstance().messageSent(client, Protocol.ACK_TOKEN);

                                                    FileOutputStream fileout = new FileOutputStream(file_folder + "/" + fileName);
                                                    fileout.write(clientInputStream.readNBytes(filesize));
                                                    fileout.close();

                                                    controllerOut.println(Protocol.STORE_ACK_TOKEN + " " + fileName);
                                                    controllerOut.flush();
                                                    DstoreLogger.getInstance().messageSent(client, Protocol.STORE_ACK_TOKEN + " " + fileName);
                                                } catch (IOException ioException) {
                                                    System.err.println(ioException);
                                                    ioException.printStackTrace();
                                                }
                                                break;
                                            }
                                            case Protocol.LOAD_DATA_TOKEN: {
                                                //get file name
                                                String fileName = commands[1];

                                                try {
                                                    File file = new File(file_folder + "/" + fileName);

                                                    if (file.exists()) {
                                                        FileInputStream inf = new FileInputStream(file);

                                                        clientOutputStream.write(inf.readAllBytes());
                                                        clientOutputStream.flush();
                                                        inf.close();
                                                    }

                                                } catch (Exception e) {
                                                    System.err.println(e);
                                                }
                                                break;
                                            }
                                            case Protocol.REBALANCE_STORE_TOKEN: {
                                                //get file name
                                                String fileName = commands[1];

                                                //get file size
                                                int filesize = Integer.parseInt(commands[2]);

                                                //here the client is actually a dstore
                                                try {
                                                    clientOut.println(Protocol.ACK_TOKEN);
                                                    clientOut.flush();
                                                    DstoreLogger.getInstance().messageSent(client, Protocol.ACK_TOKEN);

                                                    //find the file content and write it to file
                                                    FileOutputStream fileout = new FileOutputStream(file_folder + "/" + fileName);
                                                    fileout.write(clientInputStream.readNBytes(filesize));
                                                    fileout.close();

                                                } catch (IOException ioException) {
                                                    System.err.println(ioException);
                                                    ioException.printStackTrace();
                                                }
                                                break;
                                            }
                                            default:
                                                //malformed command
                                                //TODO: log error and continue
                                                break;
                                        }
                                        System.out.println("waiting for more input");
                                    }
                                    System.out.println("thread ended");
                                } catch (IOException e) {
                                    System.err.println(e);
                                }
                            } catch (IOException e) {
                                System.err.println(e);
                            }
                        }
                    }).start();
                } catch (Exception e) {
                    System.err.println(e);
                }
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }
}