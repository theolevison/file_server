import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


public class Controller {

    private final ConcurrentHashMap<String, FileObject> index = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> storeAcks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> removeAcks = new ConcurrentHashMap<>();
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

    private final AtomicBoolean rebalanceAllowed = new AtomicBoolean(true);
    private final AtomicBoolean rebalancing = new AtomicBoolean( false);

    public static void main(String[] args) throws IOException {

        Controller controller = new Controller();
        ControllerLogger.init(Logger.LoggingType.ON_FILE_AND_TERMINAL);

        controller.cport = Integer.parseInt(args[0]);
        controller.R = Integer.parseInt(args[1]);
        controller.timeout = Integer.parseInt(args[2]);
        controller.rebalance_period = Integer.parseInt(args[3]);


        new Thread(new Runnable(){
            public void run() {
                System.out.println("starting rebalance timer");
                while (true) {
                    try {
                        Thread.sleep(controller.rebalance_period);
                        controller.rebalance();
                    } catch (InterruptedException | IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

        //start server and keep checking for connections
        try {
            ServerSocket clientSocket = new ServerSocket(controller.cport);

            for (int i = 0; i < controller.R; i++) {
                System.out.println("waiting for connection from " + (controller.R-i) + " dstore/s");
                Socket client = clientSocket.accept();

                InputStream clientInputStream = client.getInputStream();
                BufferedReader clientIn = new BufferedReader(new InputStreamReader(clientInputStream));
                String input = clientIn.readLine();
                ControllerLogger.getInstance().messageReceived(client, input);
                //find the command
                String[] commands = input.split(" ");

                if (commands[0].equals(Protocol.JOIN_TOKEN)) {
                    //get port
                    String port = commands[1];
                    DStoreObject dstore = new DStoreObject(Integer.parseInt(port), client, clientInputStream, client.getOutputStream());
                    controller.startThreadOnDstore(client, clientInputStream, dstore);
                    controller.dStores.add(dstore);
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
                                    while (controller.rebalancing.get()){
                                        //wait for rebalance to complete
                                    }
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

    private void startThreadOnDstore(Socket client, InputStream dstoreInputStream, DStoreObject dstore){
        new Thread(new Runnable(){
            public void run() {

                try {
                    BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstoreInputStream));
                    String input;

                    while ((input = dstoreIn.readLine()) != null) {
                        String[] commands = input.split(" ");
                        ControllerLogger.getInstance().messageReceived(client, input);
                        switch (commands[0]) {
                            case Protocol.STORE_ACK_TOKEN: {
                                storeAcks.put(commands[1], storeAcks.get(commands[1]) + 1);
                                break;
                            }
                            case Protocol.REMOVE_ACK_TOKEN:{
                                removeAcks.put(commands[1], removeAcks.get(commands[1]) + 1);
                            }
                            default: {
                                break;
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println(e);
                }
                System.err.println("dstore closed unexpectedly");
                dStores.remove(dstore);
            }
        }).start();
    }

    private void doOperations(Socket client, String[] commands, PrintWriter clientOut, OutputStream clientOutputStream, BufferedReader clientIn, InputStream clientInputStream) throws IOException {

        switch (commands[0]) {
            case Protocol.STORE_TOKEN: {
                rebalanceAllowed.set(false);
                String fileName = commands[1];
                int filesize = Integer.parseInt(commands[2]);

                //update index
                if (index.putIfAbsent(fileName, new FileObject(fileName, filesize, STORE_IN_PROGRESS)) != null) {
                    clientOut.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                    clientOut.flush();
                    ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                } else {
                    //select R dstores
                    //build string of dstore ports
                    if (dStores.size() < R) {
                        clientOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        clientOut.flush();
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        index.remove(fileName);
                    } else {
                        storeAcks.put(fileName, 0);
                        StringBuilder outputMsg = new StringBuilder(Protocol.STORE_TO_TOKEN + " ");
                        List<DStoreObject> sublist = dStores.subList(0, R);
                        sublist.forEach(v -> outputMsg.append(v.getPort()).append(" "));

                        //send the list of ports to client
                        clientOut.println(outputMsg);
                        clientOut.flush();
                        ControllerLogger.getInstance().messageSent(client, outputMsg.toString());

                        //check acks from all the dstores
                        long startTime = System.currentTimeMillis();
                        boolean flag = false;
                        do {
                            if(storeAcks.get(fileName)>=R){
                                flag=true;
                            }
                        } while (System.currentTimeMillis()-startTime<timeout/2);

                        if (flag){
                            //change status and update client
                            index.get(fileName).setStatus(STORE_COMPLETE);
                            storeAcks.remove(fileName);
                            clientOut.println(Protocol.STORE_COMPLETE_TOKEN);
                            clientOut.flush();
                            ControllerLogger.getInstance().messageSent(client, Protocol.STORE_COMPLETE_TOKEN);
                        } else {
                            index.remove(fileName);
                            storeAcks.remove(fileName);
                            System.err.println("store failed, removing " + fileName);
                        }
                    }
                }
                rebalanceAllowed.set(true);
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
                rebalanceAllowed.set(false);
                //get file name
                String fileName = commands[1];

                if (index.containsKey(fileName)) {
                    if (index.get(fileName).getStatus().equals(STORE_IN_PROGRESS) || index.get(fileName).getStatus().equals(REMOVE_IN_PROGRESS) ) {
                        clientOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                        System.out.println("file does not exist");
                        clientOut.flush();
                        ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    } else {
                        if (dStores.size() < R) {
                            clientOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                            clientOut.flush();
                            ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                        } else {
                            index.get(fileName).setStatus(REMOVE_IN_PROGRESS);
                            removeAcks.put(fileName, 0);
                            for (DStoreObject dstore : dStores) {
                                PrintWriter dstoreOut = new PrintWriter(dstore.getOutputStream());
                                dstoreOut.println(Protocol.REMOVE_TOKEN + " " + fileName);
                                dstoreOut.flush();
                                if (dstoreOut.checkError()){
                                    System.err.println("dstore closed unexpectedly during remove");
                                    dStores.remove(dstore);
                                }
                                ControllerLogger.getInstance().messageSent(dstore.getSocket(), Protocol.REMOVE_TOKEN + " " + fileName);
                            }

                            //check acks from all the dstores
                            long startTime = System.currentTimeMillis();
                            boolean flag = false;
                            do {
                                if(removeAcks.get(fileName)>=R){
                                    flag=true;
                                }
                            } while (System.currentTimeMillis()-startTime<timeout/2);

                            if (flag) {
                                index.get(fileName).setStatus(REMOVE_COMPLETE);
                                index.remove(fileName);
                                clientOut.println(Protocol.REMOVE_COMPLETE_TOKEN);
                                clientOut.flush();
                                ControllerLogger.getInstance().messageSent(client, Protocol.REMOVE_COMPLETE_TOKEN);
                            } else {
                                System.err.println("not all dstores ackd during remove");
                            }
                        }
                    }
                } else {
                    clientOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                    clientOut.flush();
                    ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                }
                rebalanceAllowed.set(true);
                break;
            }
            case Protocol.LIST_TOKEN:
                if (dStores.size() < R) {
                    clientOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    clientOut.flush();
                    ControllerLogger.getInstance().messageSent(client, Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                } else {
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
                }

                break;
            case Protocol.JOIN_TOKEN:
                String port = commands[1];

                DStoreObject dstore = new DStoreObject(Integer.parseInt(port), client, clientInputStream, client.getOutputStream());
                startThreadOnDstore(client, clientInputStream, dstore);
                dStores.add(dstore);
                ControllerLogger.getInstance().dstoreJoined(client, Integer.parseInt(port));

                rebalance();
                break;
            default:
                //malformed command
                break;
        }
    }

    private void rebalance() throws IOException {
        if (rebalanceAllowed.get()) {
            if (rebalancing.compareAndSet(false, true)) { //check that no other rebalance operations are happening
                System.out.println("rebalancing");
                HashMap<String, ArrayList<DStoreObject>> fileLocations = new HashMap<>();
                HashMap<DStoreObject, ArrayList<String>> fileLocations2 = new HashMap<>();
                try {
                    //find where files are stored
                    for (DStoreObject dstore : dStores) {
                        try {
                            Socket socket = new Socket(InetAddress.getLocalHost(), dstore.getPort());
                            socket.setSoTimeout(timeout);
                            OutputStream dstoreOutputStream = socket.getOutputStream();
                            PrintWriter dstoreOut = new PrintWriter(dstoreOutputStream);
                            InputStream dstoreInputStream = socket.getInputStream();
                            BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstoreInputStream));

                            dstoreOut.println(Protocol.LIST_TOKEN);
                            dstoreOut.flush();
                            ControllerLogger.getInstance().messageSent(dstore.getSocket(), Protocol.LIST_TOKEN);

                            if (dstoreOut.checkError()) {
                                System.err.println("dstore closed unexpectedly during rebalance");
                                dStores.remove(dstore);
                            } else {

                                String input = dstoreIn.readLine();
                                ControllerLogger.getInstance().messageReceived(dstore.getSocket(), input);

                                String[] commands = input.split(" ");

                                if (commands[0].equals(Protocol.LIST_TOKEN)) {
                                    String[] listOfFilesInDstore = Arrays.copyOfRange(commands, 1, commands.length);//TODO: decide if this will be out of bounds
                                    fileLocations2.put(dstore, new ArrayList<>(List.of(listOfFilesInDstore)));
                                    Arrays.stream(listOfFilesInDstore).forEach(file -> {
                                        if (fileLocations.containsKey(file)) {
                                            fileLocations.get(file).add(dstore);
                                        } else {
                                            fileLocations.put(file, new ArrayList<>(List.of(dstore)));
                                        }
                                    });
                                }
                                System.out.println(fileLocations);
                            }
                            socket.close();
                        } catch (SocketTimeoutException e){
                            System.err.println("dstore timed out during rebalance list");
                        }
                    }

                    //share files out between dstores
                    HashMap<String, ArrayList<DStoreObject>> newFileLocations = new HashMap<>();
                    int count = 0;
                    for (String file : fileLocations.keySet()) {
                        newFileLocations.put(file, new ArrayList<>());
                        for (int i = 0; i < R; i++) {
                            if (count >= dStores.size()) {
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
                        ArrayList<DStoreObject> copyOfFileLocationsForFile = (ArrayList<DStoreObject>) fileLocations.get(file).clone();
                        ArrayList<DStoreObject> copyOfNewFileLocationsForFile = (ArrayList<DStoreObject>) newFileLocations.get(file).clone();

                        //dont remove files you are meant to have
                        copyOfFileLocationsForFile.removeAll(newFileLocations.get(file));
                        copyOfFileLocationsForFile.forEach(dstore -> {
                            if (filesToRemove.containsKey(dstore)) {
                                filesToRemove.get(dstore).add(file);
                            } else {
                                filesToRemove.put(dstore, new ArrayList<>(List.of(file)));
                            }
                        });


                        //dont send files to dstores that already have them
                        copyOfNewFileLocationsForFile.removeAll(fileLocations.get(file));
                        copyOfNewFileLocationsForFile.forEach(dstore -> {
                            if (filesToSend.containsKey(file)) {
                                filesToSend.get(file).add(dstore);
                            } else {
                                filesToSend.put(file, new ArrayList<>(List.of(dstore)));
                            }
                        });

                    }

                    for (DStoreObject dstore : dStores) {
                        try {
                            Socket socket = new Socket(InetAddress.getLocalHost(), dstore.getPort());
                            socket.setSoTimeout(timeout);
                            OutputStream dstoreOutputStream = socket.getOutputStream();
                            PrintWriter dstoreOut = new PrintWriter(dstoreOutputStream);
                            InputStream dstoreInputStream = socket.getInputStream();
                            BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstoreInputStream));

                            //files to send
                            StringBuilder outputMsg = new StringBuilder(Protocol.REBALANCE_TOKEN + " ");
                            StringBuilder sendy = new StringBuilder();

                            //count how many times dstore appears in filesToSend
//                            Set<String> keys = ((HashMap<String, ArrayList<DStoreObject>>) fileLocations.clone()).keySet();
//                            fileLocations2.get(dstore).forEach(keys::remove);
//                            outputMsg.append(keys.size()).append(" ");
                            AtomicInteger county = new AtomicInteger();

                            System.err.println(fileLocations);
                            System.err.println(filesToSend);

                            filesToSend.forEach((k, v) -> {
                                if (fileLocations.get(k).contains(dstore)) {
                                    sendy.append(k).append(" ").append(v.size()).append(" ");
                                    filesToSend.get(k).forEach(dstore2 -> sendy.append(dstore2.getPort()).append(" "));
                                    county.getAndIncrement();
                                }
                            });

                            outputMsg.append(county).append(" ").append(sendy);

                            //files to remove
                            if (!filesToRemove.containsKey(dstore)) {
                                outputMsg.append(0);
                            } else {
                                outputMsg.append(filesToRemove.get(dstore).size()).append(" ");
                                filesToRemove.get(dstore).forEach(file -> outputMsg.append(file).append(" "));
                            }

                            dstoreOut.println(outputMsg);
                            dstoreOut.flush();
                            ControllerLogger.getInstance().messageSent(socket, outputMsg.toString());

                            //find the command
                            String input = dstoreIn.readLine();
                            ControllerLogger.getInstance().messageReceived(socket, input);
                            String[] commands = input.split(" ");

                            if (!commands[0].equals(Protocol.REBALANCE_COMPLETE_TOKEN)) {
                                System.err.println("dstore didnt rebalance complete during rebalance");
                            }
                            socket.close();
                        } catch (SocketTimeoutException e){
                            System.err.println("dstore timed out during rebalance sending");
                        }
                    }
                } catch (SocketTimeoutException e){
                    System.out.println(e);
                }
                rebalancing.set(false);
                System.out.println("rebalancing complete");
            }
        }
    }
}
