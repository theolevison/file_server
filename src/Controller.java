import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;


public class Controller {

    private final ConcurrentHashMap<String, FileObject> index = new ConcurrentHashMap<>();
    //Map<String, FileObject> index = Collections.synchronizedMap(indexNonSynced);
    private final CopyOnWriteArrayList<DStoreObject> dStores = new CopyOnWriteArrayList<>();
    //private final ArrayList<DStoreObject> dStores = new ArrayList<>();
    int cport;
    int R;
    int timeout;
    int rebalance_period;
    public final static String STORE_IN_PROGRESS = "store in progress";
    public final static String STORE_COMPLETE = "store complete";
    public final static String REMOVE_IN_PROGRESS = "remove in progress";
    public final static String REMOVE_COMPLETE = "remove complete";

    //TODO: monitor when dstores timeout

    public static void main(String[] args) throws IOException {

        Controller controller = new Controller();
        ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);

        controller.cport = Integer.parseInt(args[0]);
        controller.R = Integer.parseInt(args[1]);
        controller.timeout = Integer.parseInt(args[2]);
        controller.rebalance_period = Integer.parseInt(args[3]);

        //start server and keep checking for connections
        try {
            ServerSocket clientSocket = new ServerSocket(controller.cport);

            for (int i = 0; i < controller.R; i++) {
                System.out.println("waiting for connection from " + (controller.R-i) + " dstore/s");
                Socket client = clientSocket.accept();
                System.out.println("dstore connected");

                InputStream clientInputStream = client.getInputStream();
                BufferedReader clientIn = new BufferedReader(new InputStreamReader(clientInputStream));
                String input = clientIn.readLine();
                ControllerLogger.getInstance().messageReceived(client, input);
                //find the command
                String[] commands = input.split(" ");
                System.out.println("command " + Arrays.toString(commands));

                if (commands[0].equals(Protocol.JOIN_TOKEN)) {
                    //get port
                    String port = commands[1];
                    System.out.println("new dstore joined on port " + port);

                    controller.dStores.add(new DStoreObject(Integer.parseInt(port), client, clientInputStream, client.getOutputStream()));
                    System.out.println("dstore joined successfully");
                    ControllerLogger.getInstance().dstoreJoined(client, Integer.parseInt(port));
                }
            }


            for (; ; ) {
                try {
                    System.out.println("waiting for connection");
                    Socket client = clientSocket.accept();

                    new Thread(new Runnable(){
                        public void run() {
                            System.out.println("new thread");

                            try {
                                InputStream clientInputStream = client.getInputStream();
                                BufferedReader clientIn = new BufferedReader(new InputStreamReader(clientInputStream));
                                OutputStream clientOutputStream = client.getOutputStream();
                                PrintWriter clientOut = new PrintWriter(clientOutputStream);

                                String input;

                                while ((input = clientIn.readLine()) != null) {
                                    ControllerLogger.getInstance().messageReceived(client, input);
                                    controller.doOperations(client, input.split(" "), clientOut, clientOutputStream, clientIn, clientInputStream);
                                }
                            } catch (Exception e) {
                                System.err.println(e);
                            }
                        }
                    }).start();
                } catch(Exception e){
                    System.err.println(e);
                }
            }
        } catch (Exception e) {
            System.err.println(e);
        }
    }

    private void doOperations(Socket client, String[] commands, PrintWriter clientOut, OutputStream clientOutputStream, BufferedReader clientIn, InputStream clientInputStream) throws IOException {

        System.out.println("command " + Arrays.toString(commands));

        switch (commands[0]) {
            case Protocol.STORE_TOKEN: {
                String fileName = commands[1];
                int filesize = Integer.parseInt(commands[2]);

                //update index
                if (index.containsKey(fileName)){
                    clientOut.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    System.out.println("file already exists");
                    clientOut.flush();
                    ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                } else {
                    index.put(fileName, new FileObject(fileName, filesize, STORE_IN_PROGRESS));
                    //select R dstores
                    //build string of dstore ports
                    if (dStores.size() < R) {
                        clientOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        index.remove(fileName);
                        clientOut.flush();
                    } else {
                        StringBuilder outputMsg = new StringBuilder(Protocol.STORE_TO_TOKEN + " ");
                        List<DStoreObject> sublist = dStores.subList(0, R);
                        sublist.forEach(v -> outputMsg.append(v.getPort()).append(" "));

                        try {
                            System.out.println("send ports to client");
                            //send the list of ports to client
                            clientOut.println(outputMsg);
                            clientOut.flush();
                            ControllerLogger.getInstance().messageSent(client, outputMsg.toString());

                            //check acks from all the dstores
                            boolean flag = false;
                            for (DStoreObject dstore : sublist) {
                                BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstore.getSocket().getInputStream()));
                                String ack = dstoreIn.readLine();
                                ControllerLogger.getInstance().messageReceived(dstore.getSocket(), ack);

                                if (!ack.equals(Protocol.STORE_ACK_TOKEN + " " + fileName)) {
                                    //no ack
                                    flag = true;
                                }
                            }
                            //change status and update client
                            if (flag) {
                                index.remove(fileName);
                                System.out.println("store failed, removing " + fileName);
                            } else {
                                index.get(fileName).setStatus(STORE_COMPLETE);
                                System.out.println(STORE_COMPLETE);
                                clientOut.println(Protocol.STORE_COMPLETE_TOKEN);
                                clientOut.flush();
                                ControllerLogger.getInstance().messageSent(client, Protocol.STORE_COMPLETE_TOKEN);
                            }

                        } catch (IOException ioException) {
                            ioException.printStackTrace();
                            System.err.println("something went wrong");
                        }
                    }
                }
                break;
            }
            case Protocol.LOAD_TOKEN: {
                //get file name
                String fileName = commands[1];
                System.out.println("fileName " + fileName);

                if (index.containsKey(fileName)) {
                    if (index.get(fileName).getStatus().equals(STORE_IN_PROGRESS) || index.get(fileName).getStatus().equals(REMOVE_IN_PROGRESS) ) {
                        clientOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        System.out.println("file does not exist");
                        clientOut.flush();
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    } else {
                        try {
                            int count = 0;
                            DStoreObject dstore = dStores.get(count);

                            try {
                                clientOut.println(Protocol.LOAD_FROM_TOKEN + " " + dstore.getPort() + " " + index.get(fileName).getSize());
                                clientOut.flush();
                                ControllerLogger.getInstance().messageSent(client, Protocol.LOAD_FROM_TOKEN + " " + dstore.getPort() + " " + index.get(fileName).getSize());

                                String input;
                                while ((input = clientIn.readLine()) != null) {
                                    commands = input.split(" ");
                                    if (commands[0].equals(Protocol.RELOAD_TOKEN)) {
                                        ControllerLogger.getInstance().messageReceived(client, input);
                                        //get file name
                                        fileName = commands[1];
                                        System.out.println("fileName " + fileName);

                                        try {
                                            dstore = dStores.get(count);
                                        } catch (IndexOutOfBoundsException e) {
                                            clientOut.println(Protocol.ERROR_LOAD_TOKEN);
                                            clientOut.flush();
                                            ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_LOAD_TOKEN);
                                        }
                                        count++;
                                        clientOut.println(Protocol.LOAD_FROM_TOKEN + " " + dstore.getPort() + " " + index.get(fileName).getSize());
                                        clientOut.flush();
                                        ControllerLogger.getInstance().messageSent(client, Protocol.LOAD_FROM_TOKEN + " " + dstore.getPort() + " " + index.get(fileName).getSize());
                                    } else {
                                        doOperations(client, commands, clientOut, clientOutputStream, clientIn, clientInputStream);
                                        return;
                                    }
                                }
                            } catch (Exception e) {
                                System.err.println(e);
                            }
                        } catch (IndexOutOfBoundsException e) {
                            clientOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                            clientOut.flush();
                            ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        }
                    }
                } else {
                    clientOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    clientOut.flush();
                    ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }

                break;
            }
            case Protocol.REMOVE_TOKEN: {
                //get file name
                String fileName = commands[1];
                System.out.println("fileName " + fileName);

                if (index.containsKey(fileName)) {
                    if (index.get(fileName).getStatus().equals(STORE_IN_PROGRESS) || index.get(fileName).getStatus().equals(REMOVE_IN_PROGRESS) ) {
                        clientOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        System.out.println("file does not exist");
                        clientOut.flush();
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    } else {
                        index.get(fileName).setStatus(REMOVE_IN_PROGRESS);
                        int count = 0;

                        if (dStores.size() < R) {
                            clientOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                            clientOut.flush();
                            ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        } else {
                            for (DStoreObject dstore : dStores) {
                                PrintWriter dstoreOut = new PrintWriter(dstore.getOutputStream());
                                dstoreOut.println(Protocol.REMOVE_TOKEN + " " + fileName);
                                dstoreOut.flush();
                                ControllerLogger.getInstance().messageSent(dstore.getSocket(), Protocol.REMOVE_TOKEN + " " + fileName);

                                System.out.println("send remove to dstore " + dstore.getPort());

                                BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstore.getInputStream()));
                                String input = dstoreIn.readLine();
                                ControllerLogger.getInstance().messageReceived(dstore.getSocket(), input);

                                if (input.equals(Protocol.REMOVE_ACK_TOKEN + " " + fileName)) {
                                    count++;
                                } else {
                                    //TODO: log error, malformed command
                                    System.out.println("remove error");
                                }
                            }

                            if (count == R) {
                                index.get(fileName).setStatus(REMOVE_COMPLETE);
                                index.remove(fileName);
                                clientOut.println(Protocol.REMOVE_COMPLETE_TOKEN);
                                clientOut.flush();
                                ControllerLogger.getInstance().messageSent(client, Protocol.REMOVE_COMPLETE_TOKEN);
                            } else {
                                //TODO: log error not all dstores ack
                            }
                        }
                    }
                } else {
                    clientOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    clientOut.flush();
                    ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
                break;
            }
            case Protocol.LIST_TOKEN: //TODO: fix error where list waits on index to complete stores, but timesout and thus causes null pointer exception in client
                StringBuilder outputMsg = new StringBuilder(Protocol.LIST_TOKEN + " ");
                for (FileObject v : index.values()) {
                    String status = v.getStatus();
                    if (!(status.equals(STORE_IN_PROGRESS) || status.equals(REMOVE_IN_PROGRESS))) {
                        outputMsg.append(v.getName()).append(" ");
                    }
                }

                clientOut.println(outputMsg);
                clientOut.flush();
                ControllerLogger.getInstance().messageSent(client, outputMsg.toString());

                break;
            case Protocol.JOIN_TOKEN:
                //get port
                String port = commands[1];
                System.out.println("new dstore joined on port " + port);

                dStores.add(new DStoreObject(Integer.parseInt(port), client, clientInputStream, clientOutputStream));
                ControllerLogger.getInstance().dstoreJoined(client, Integer.parseInt(port));

                //controller.rebalance();
                //TODO: re-enable rebalance once other systems are tested
                break;
            default:
                //malformed command
                //TODO: log error and continue
                break;
        }
    }

    //TODO: wait for store and remove to be complete, and check only one rebalance is running at the same time
    private void rebalance() throws IOException {
        System.out.println("rebalancing");
        HashMap<String, ArrayList<DStoreObject>> fileLocations = new HashMap<>();
        byte[] buf = new byte[1000];
        int buflen;

        //find where files are stored
        for (DStoreObject dstore : dStores) {
            OutputStream dstoreOut = dstore.getSocket().getOutputStream();
            dstoreOut.write(("LIST").getBytes(StandardCharsets.UTF_8));

            //find the command
            InputStream dstoreIn = dstore.getSocket().getInputStream();
            buflen = dstoreIn.read(buf);
            String firstBuffer = new String(buf, 0, buflen-1);
            int firstSpace = firstBuffer.indexOf(" ");
            String command = firstBuffer.substring(0, firstSpace);
            System.out.println("command " + command);

            if (command.equals(Protocol.LIST_TOKEN)) {
                String[] listOfFilesInDstore = firstBuffer.substring(firstSpace + 1, buflen-1).split(" ");
                Arrays.stream(listOfFilesInDstore).forEach(file -> {
                    if (fileLocations.containsKey(file)){
                        fileLocations.get(file).add(dstore);
                    } else {
                        fileLocations.put(file, new ArrayList<>(List.of(dstore)));
                    }
                });
            }
        }

        //share files out between dstores
        HashMap<String, ArrayList<DStoreObject>> newFileLocations = new HashMap<>();
        int count = 0;
        for (String file : fileLocations.keySet()) {
            newFileLocations.put(file, new ArrayList<>());
            for (int i = 0; i < R; i++) {
                if (count >= dStores.size()){
                    count = 0;
                }
                newFileLocations.get(file).add(dStores.get(count));
                count++;
            }
        }

        //generate lists to remove and send
        HashMap<String, ArrayList<DStoreObject>> filesToSend = new HashMap<>();
        HashMap<DStoreObject, ArrayList<String>> filesToRemove = new HashMap<>();
        for (String file : fileLocations.keySet()) {
            ArrayList<DStoreObject> copyOfFileLocations = fileLocations.get(file);
            ArrayList<DStoreObject> copyOfNewFileLocations = newFileLocations.get(file);
            copyOfFileLocations.removeAll(newFileLocations.get(file));
            copyOfFileLocations.forEach(dstore -> {
                if (filesToRemove.containsKey(dstore)){
                    filesToRemove.get(dstore).add(file);
                } else {
                    filesToRemove.put(dstore, new ArrayList<>(List.of(file)));
                }
            });

            copyOfNewFileLocations.removeAll(fileLocations.get(file));
            copyOfNewFileLocations.forEach(dstore -> {
                if (filesToSend.containsKey(file)){
                    filesToSend.get(file).add(dstore);
                } else {
                    filesToSend.put(file, new ArrayList<>(List.of(dstore)));
                }
            });
        }

        for (DStoreObject dstore : dStores) {
            OutputStream dstoreOut = dstore.getSocket().getOutputStream();

            //files to send
            StringBuilder outputMsg = new StringBuilder(Protocol.REBALANCE_TOKEN + " ");
            filesToSend.forEach((k,v) -> {
                if (fileLocations.get(k).contains(dstore)) {
                    outputMsg.append(k).append(v.size()).append(" ");
                    filesToSend.get(k).forEach(dstore2 -> outputMsg.append(dstore2.getPort()).append(" "));
                }
            });

            //files to remove
            outputMsg.append(filesToRemove.get(dstore).size()).append(" ");
            filesToRemove.get(dstore).forEach(file -> outputMsg.append(file).append(" "));

            dstoreOut.write(outputMsg.toString().getBytes(StandardCharsets.UTF_8));
            dstoreOut.close();

            //TODO: test against timeout
            //find the command
            InputStream dstoreIn = dstore.getSocket().getInputStream();
            buflen = dstoreIn.read(buf);
            String firstBuffer = new String(buf, 0, buflen-1);
            int firstSpace = firstBuffer.indexOf(" ");
            String command = firstBuffer.substring(0, firstSpace);
            System.out.println("command " + command);

            if (!command.equals(Protocol.REMOVE_COMPLETE_TOKEN)) {
                //TODO: log error
            }
        }
    }
}
