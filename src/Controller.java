import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class Controller {

    private Hashtable<String, FileObject> indexNonSynced = new Hashtable<>();
    Map<String, FileObject> index = Collections.synchronizedMap(indexNonSynced);
    private ArrayList<DStoreObject> dStores = new ArrayList<>(); //TODO: make this synchronized
    int cport;
    int R;
    int timeout;
    int rebalance_period;

    public static void main(String[] args) throws IOException {

        Controller controller = new Controller();

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
                //find the command
                String[] commands = input.split(" ");
                System.out.println("command " + Arrays.toString(commands));

                if (commands[0].equals(Protocol.JOIN_TOKEN)) {
                    //get port
                    String port = commands[1];
                    System.out.println("new dstore joined on port " + port);

                    controller.dStores.add(new DStoreObject(Integer.parseInt(port), client));
                    System.out.println("dstore joined successfully");
                }
            }


            for (; ; ) {
                try {
                    System.out.println("waiting for connection");
                    Socket client = clientSocket.accept();
                    //TODO: wait for R dstores to join the system


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
                                    controller.doOperations(client, input.split(" "), clientOut, clientOutputStream, clientIn, clientInputStream);
                                }
                            } catch (Exception e) {
                                System.out.println(e);
                            }
                        }
                    }).start();
                } catch(Exception e){
                    System.out.println(e);
                }
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private void doOperations(Socket client, String[] commands, PrintWriter clientOut, OutputStream clientOutputStream, BufferedReader clientIn, InputStream clientInputStream) throws IOException {
        byte[] buf = new byte[1000];
        int buflen;

        System.out.println("command " + Arrays.toString(commands));

        if (commands[0].equals(Protocol.STORE_TOKEN)) {
            //get file name
            String fileName = commands[1];
            System.out.println("fileName " + fileName);

            //get file size
            int filesize = Integer.parseInt(commands[2]);
            System.out.println("fileSize " + filesize);

            //update index
            if (index.putIfAbsent(fileName, new FileObject(fileName, filesize, "store in progress")) != null) {
                clientOut.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                clientOut.flush();
                //clientOut.write("ERROR_FILE_ALREADY_EXISTS".getBytes(StandardCharsets.UTF_8));
            } else {
                //select R dstores
                try {
                    //build string of dstore ports
                    StringBuilder outputMsg = new StringBuilder(Protocol.STORE_TO_TOKEN+" ");
                    List<DStoreObject> sublist = dStores.subList(0, R);
                    sublist.forEach(v -> outputMsg.append(v.getPort()).append(" "));

                    try {
                        System.out.println("send ports to client");
                        //send the list of ports to client
                        clientOut.println(outputMsg);
                        clientOut.flush();
                        //clientOut.write(outputMsg.toString().getBytes(StandardCharsets.UTF_8));

                        //check acks from all the dstores
                        boolean flag = false;
                        for (DStoreObject dstore : sublist) {
                            BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstore.getSocket().getInputStream()));
                            String ack = dstoreIn.readLine();

                            if (!ack.equals(Protocol.STORE_ACK_TOKEN + " " + fileName)) {
                                //no ack
                                System.out.println("no ack");
                                flag = true;
                            } else {
                                System.out.println("ack received");
                            }
                            //dstoreIn.close();
                        }
                        //change status and update client
                        if (flag) {
                            index.remove(fileName);
                        } else {
                            index.get(fileName).setStatus("store complete");
                            System.out.println("store complete");
                            clientOut.println(Protocol.STORE_COMPLETE_TOKEN);
                            clientOut.flush();
                            //clientOut.write("STORE_COMPLETE".getBytes(StandardCharsets.UTF_8));
                        }

                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("not enough dstores, add more");
                    clientOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    index.remove(fileName);
                    //clientOut.write("ERROR_NOT_ENOUGH_DSTORES".getBytes(StandardCharsets.UTF_8));
                    clientOut.flush();
                }
            }
        } else if (commands[0].equals(Protocol.LOAD_TOKEN)) {
            //get file name
            String fileName = commands[1];
            System.out.println("fileName " + fileName);

            if (index.containsKey(fileName)) {
                try {
                    int count = 0;
                    DStoreObject dstore = dStores.get(count);

                    try {
                        clientOut.println(Protocol.LOAD_FROM_TOKEN + " " + dstore.getPort() + " " + index.get(fileName).getSize());
                        clientOut.flush();

                        String input;
                        while ((input = clientIn.readLine()) != null) {
                            commands = input.split(" ");
                            if (commands[0].equals(Protocol.RELOAD_TOKEN)){
                                //get file name
                                fileName = commands[1];
                                System.out.println("fileName " + fileName);

                                try {
                                    dstore = dStores.get(count);
                                } catch (IndexOutOfBoundsException e) {
                                    clientOut.println(Protocol.ERROR_LOAD_TOKEN);
                                    clientOut.flush();
                                    //clientOut.write(("ERROR_LOAD").getBytes(StandardCharsets.UTF_8));
                                }
                                count++;
                                clientOut.println(Protocol.LOAD_FROM_TOKEN + " " + dstore.getPort() + " " + index.get(fileName).getSize());
                                clientOut.flush();
                            } else {
                                doOperations(client, commands, clientOut, clientOutputStream, clientIn, clientInputStream);
                                return;
                            }
                        }
                    } catch (Exception e) {
                        System.out.println(e);
                    }
                } catch (IndexOutOfBoundsException e) {
                    clientOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    clientOut.flush();
                }
            } else {
                clientOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                clientOut.flush();
            }

        } else if (commands[0].equals(Protocol.REMOVE_TOKEN)) {
            //get file name
            String fileName = commands[1];
            System.out.println("fileName " + fileName);

            if (index.containsKey(fileName)) {
                index.get(fileName).setStatus("remove in progress");

                int count = 0;

                if (dStores.size() < R) {
                    clientOut.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                    clientOut.flush();
                } else {
                    for (DStoreObject dstore : dStores) {
                        OutputStream dstoreOut = dstore.getSocket().getOutputStream();
                        dstoreOut.write((Protocol.REMOVE_TOKEN + " " + fileName).getBytes(StandardCharsets.UTF_8));

                        InputStream dstoreIn = dstore.getSocket().getInputStream();
                        buflen = dstoreIn.read(buf);

                        String ack = new String(buf, 0, buflen - 1);

                        if (ack.equals(Protocol.REMOVE_ACK_TOKEN + " " + fileName)) {
                            count++;
                        } else {
                            //TODO: log error, malformed command
                        }
                        dstoreIn.close();
                        dstoreOut.close();
                    }

                    if (count == R) {
                        index.get(fileName).setStatus("remove complete");
                        clientOut.println(Protocol.REMOVE_COMPLETE_TOKEN);
                        //clientOut.write(("REMOVE_COMPLETE").getBytes(StandardCharsets.UTF_8));
                        clientOut.flush();
                    } else {
                        //TODO: log error not all dstores ack
                    }
                }
            } else {
                //clientOut.write(("ERROR_FILE_DOES_NOT_EXIST").getBytes(StandardCharsets.UTF_8));
                clientOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                clientOut.flush();
            }
        } else if (commands[0].equals(Protocol.LIST_TOKEN)) {
            StringBuilder outputMsg = new StringBuilder(Protocol.LIST_TOKEN + " ");
            index.forEach((k, v) -> outputMsg.append(v.getName()).append(" "));

            //clientOut.write(outputMsg.toString().getBytes(StandardCharsets.UTF_8));
            clientOut.println(outputMsg);
            System.out.println(outputMsg);
            //clientOut.close();
            clientOut.flush();

        } else if (commands[0].equals(Protocol.JOIN_TOKEN)) {
            //get port
            String port = commands[1];
            System.out.println("new dstore joined on port " + port);

            dStores.add(new DStoreObject(Integer.parseInt(port), client));

            //controller.rebalance();
            //TODO: re-enable rebalance once other systems are tested
        } else {
            //malformed command
            //TODO: log error and continue
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
