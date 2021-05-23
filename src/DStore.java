import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
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
                            } else if (commands[0].equals(Protocol.REBALANCE_TOKEN)) {
                                Boolean complete = true;
                                //find dstores to send stuff to
                                int numberToSend = Integer.parseInt(commands[1]);
                                System.out.println("number of files to send " + numberToSend);

                                int currentPos = 2;
                                for (int i = 0; i < numberToSend; i++) {

                                    String fileToSend = commands[currentPos];
                                    System.out.println("file to send " + fileToSend);
                                    currentPos++;

                                    int numberOfDstores = Integer.parseInt(commands[currentPos]);
                                    currentPos++;
                                    for (int j = 0; j < numberOfDstores; j++) {
                                        String dstore = commands[currentPos+j];
                                        System.out.println("dstore that needs a file on port " + dstore);

                                        try {
                                            Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), Integer.parseInt(dstore));
                                            InputStream dstoreInputStream = dstoreSocket.getInputStream();
                                            BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstoreInputStream));
                                            OutputStream dstoreOutputStream = dstoreSocket.getOutputStream();
                                            PrintWriter dstoreOut = new PrintWriter(dstoreOutputStream);

                                            File file = new File(file_folder + "/" + fileToSend);

                                            if (file.exists()) {
                                                dstoreOut.println(Protocol.REBALANCE_STORE_TOKEN+ " " + fileToSend + " " + file.length());
                                                dstoreOut.flush();
                                                DstoreLogger.getInstance().messageSent(dstoreSocket, Protocol.REBALANCE_STORE_TOKEN+ " " + fileToSend + " " + file.length());

                                                dstoreSocket.setSoTimeout(timeout);
                                                try {
                                                    //find the command
                                                    input = dstoreIn.readLine();
                                                    DstoreLogger.getInstance().messageReceived(dstoreSocket, input);
                                                    String[] commands2 = input.split(" ");

                                                    if (commands2[0].equals("ACK")) {
                                                        FileInputStream inf = new FileInputStream(file);

                                                        dstoreOutputStream.write(inf.readAllBytes());
                                                        dstoreOutputStream.flush();
                                                        inf.close();
                                                    } else {
                                                        System.err.println("no ack when rebalance storing");
                                                        complete = false;
                                                    }
                                                } catch (SocketTimeoutException e){
                                                    System.err.println("dstore didnt ack during rebalance");
                                                    complete = false;
                                                }
                                            }
                                            dstoreSocket.close();
                                        } catch (Exception e) {
                                            System.out.println(e);
                                        }
                                    }
                                    currentPos+=numberOfDstores;
                                }

                                //find what to delete
                                int numberToRemove = Integer.parseInt(commands[currentPos]);
                                currentPos++;
                                for (int i = 0; i < numberToRemove; i++) {
                                    String file = commands[currentPos+i];

                                    //remove file from server
                                    File file2 = new File(file_folder + "/" + file);
                                    if (!file2.delete()) {
                                        System.err.println("not able to delete " + file + " during rebalance");
                                        complete = false;
                                    }
                                }

                                if (complete) {
                                    controllerOut.println(Protocol.REBALANCE_COMPLETE_TOKEN);
                                    DstoreLogger.getInstance().messageSent(controller, Protocol.REBALANCE_COMPLETE_TOKEN);
                                }
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
                                    String input;

                                    while ((input = clientIn.readLine()) != null) {
                                        DstoreLogger.getInstance().messageReceived(client, input);
                                        System.out.println(Arrays.toString(new File(file_folder).listFiles()));

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
                                                    DstoreLogger.getInstance().messageSent(controller, Protocol.STORE_ACK_TOKEN + " " + fileName);
                                                } catch (IOException ioException) {
                                                    System.err.println(ioException);
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

                                                String fileName = commands[1];
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
                                                }
                                                break;
                                            }
                                            default:
                                                //malformed command
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