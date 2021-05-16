import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;


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

            for (; ; ) {
                try {
                    System.out.println("waiting for connection");
                    Socket client = clientSocket.accept();

                    new Thread(new Runnable(){
                        public void run() {
                            System.out.println("new thread");

                            try {
                                PrintWriter clientOut = new PrintWriter(client.getOutputStream());
                                InputStream clientIn = client.getInputStream();
                                byte[] buf = new byte[1000];
                                int buflen;

                                for (; ; ) {
                                    try {
                                        System.out.println("checking for input");

                                        while (clientIn.available()==0){

                                        }

                                        //find the command
                                        buflen = clientIn.read(buf);
                                        String firstBuffer = new String(buf, 0, buflen);
                                        int firstSpace = firstBuffer.indexOf(" ");
                                        //check if there are any spaces at all, otherwise get whole line
                                        if (firstSpace == -1) {
                                            firstSpace = buflen - 1;
                                        }
                                        String command = firstBuffer.substring(0, firstSpace);
                                        System.out.println("command " + command);

                                        if (command.equals("STORE")) {
                                            //get file name
                                            int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                                            String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
                                            System.out.println("fileName " + fileName);

                                            //get file size
                                            int filesize = Integer.parseInt(firstBuffer.substring(secondSpace + 1, buflen-1));

                                            //update index
                                            if (controller.index.putIfAbsent(fileName, new FileObject(fileName, filesize, "store in progress")) != null) {
                                                clientOut.println("ERROR_FILE_ALREADY_EXISTS");
                                                clientOut.flush();
                                                //clientOut.write("ERROR_FILE_ALREADY_EXISTS".getBytes(StandardCharsets.UTF_8));
                                            } else {
                                                //select R dstores
                                                try {
                                                    //build string of dstore ports
                                                    StringBuilder outputMsg = new StringBuilder("STORE_TO ");
                                                    List<DStoreObject> sublist = controller.dStores.subList(0, controller.R);
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
                                                            InputStream dstoreIn = dstore.getSocket().getInputStream();
                                                            buflen = dstoreIn.read(buf);

                                                            String ack = new String(buf, 0, buflen-1);

                                                            if (!ack.equals("ACK " + fileName)) {
                                                                //no ack
                                                                System.out.println("no ack");
                                                                flag = true;
                                                            }
                                                            //dstoreIn.close();
                                                        }
                                                        //change status and update client
                                                        System.out.println("test1");
                                                        if (flag) {
                                                            controller.index.remove(fileName);
                                                        } else {
                                                            controller.index.get(fileName).setStatus("store complete");
                                                            System.out.println("store complete");
                                                            clientOut.println("STORE_COMPLETE");
                                                            clientOut.flush();
                                                            //clientOut.write("STORE_COMPLETE".getBytes(StandardCharsets.UTF_8));
                                                        }

                                                    } catch (IOException ioException) {
                                                        ioException.printStackTrace();
                                                    }
                                                } catch (IndexOutOfBoundsException e) {
                                                    System.out.println("not enough dstores, add more");
                                                    clientOut.println("ERROR_NOT_ENOUGH_DSTORES");
                                                    controller.index.remove(fileName);
                                                    //clientOut.write("ERROR_NOT_ENOUGH_DSTORES".getBytes(StandardCharsets.UTF_8));
                                                    clientOut.flush();
                                                }
                                            }
                                        } else if (command.equals("LOAD")) {
                                            //get file name
                                            int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                                            String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
                                            System.out.println("fileName " + fileName);

                                            if (controller.index.containsKey(fileName)) {
                                                try {
                                                    DStoreObject dstore = controller.dStores.get(0);

                                                    try {
                                                        clientOut.println("LOAD_FROM " + dstore.getPort() + " " + controller.index.get(fileName).getSize());
                                                        clientOut.flush();
                                                        //clientOut.write(("LOAD_FROM " + dstore.getPort() + " " + controller.index.get(fileName).getSize()).getBytes(StandardCharsets.UTF_8));

                                                        int count = 1;
                                                        do {
                                                            //check if RELOAD
                                                            //find the command
                                                            buflen = clientIn.read(buf);
                                                            firstBuffer = new String(buf, 0, buflen-1);
                                                            firstSpace = firstBuffer.indexOf(" ");
                                                            command = firstBuffer.substring(0, firstSpace);
                                                            System.out.println("command " + command);

                                                            //TODO: work out if this should go in general commands to catch?
                                                            if (command.equals("RELOAD")) {
                                                                //get file name
                                                                secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                                                                fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
                                                                System.out.println("fileName " + fileName);

                                                                try {
                                                                    dstore = controller.dStores.get(count);
                                                                } catch (IndexOutOfBoundsException e) {
                                                                    clientOut.println("ERROR_LOAD");
                                                                    clientOut.flush();
                                                                    //clientOut.write(("ERROR_LOAD").getBytes(StandardCharsets.UTF_8));
                                                                }
                                                                count++;
                                                                clientOut.println("LOAD_FROM " + dstore.getPort() + " " + controller.index.get(fileName).getSize());
                                                                clientOut.flush();
                                                                //clientOut.write(("LOAD_FROM " + dstore.getPort() + " " + controller.index.get(fileName).getSize()).getBytes(StandardCharsets.UTF_8));
                                                            }
                                                        } while (command.equals("RELOAD"));
                                                        clientOut.flush();

                                                    } catch (Exception e) {
                                                        System.out.println(e);
                                                    }
                                                } catch (IndexOutOfBoundsException e) {
                                                    clientOut.println("ERROR_NOT_ENOUGH_DSTORES");
                                                    //clientOut.write(("ERROR_NOT_ENOUGH_DSTORES").getBytes(StandardCharsets.UTF_8));
                                                    clientOut.flush();
                                                }
                                            } else {
                                                clientOut.write("ERROR_FILE_DOES_NOT_EXIST");
                                                //clientOut.write(("ERROR_FILE_DOES_NOT_EXIST").getBytes(StandardCharsets.UTF_8));
                                                clientOut.flush();
                                            }

                                        } else if (command.equals("REMOVE")) {
                                            //get file name
                                            int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                                            String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
                                            System.out.println("fileName " + fileName);

                                            if (controller.index.containsKey(fileName)) {
                                                controller.index.get(fileName).setStatus("remove in progress");

                                                int count = 0;

                                                if (controller.dStores.size() < controller.R) {
                                                    clientOut.println("ERROR_NOT_ENOUGH_DSTORES");
                                                    //clientOut.write(("ERROR_NOT_ENOUGH_DSTORES").getBytes(StandardCharsets.UTF_8));
                                                    clientOut.flush();
                                                } else {
                                                    for (DStoreObject dstore : controller.dStores) {
                                                        OutputStream dstoreOut = dstore.getSocket().getOutputStream();
                                                        dstoreOut.write(("REMOVE " + fileName).getBytes(StandardCharsets.UTF_8));

                                                        InputStream dstoreIn = dstore.getSocket().getInputStream();
                                                        buflen = dstoreIn.read(buf);

                                                        String ack = new String(buf, 0, buflen-1);

                                                        if (ack.equals("REMOVE_ACK " + fileName)) {
                                                            count++;
                                                        } else {
                                                            //TODO: log error, malformed command
                                                        }
                                                        dstoreIn.close();
                                                        dstoreOut.close();
                                                    }

                                                    if (count == controller.R) {
                                                        controller.index.get(fileName).setStatus("remove complete");
                                                        clientOut.println("REMOVE_COMPLETE");
                                                        //clientOut.write(("REMOVE_COMPLETE").getBytes(StandardCharsets.UTF_8));
                                                        clientOut.flush();
                                                    } else {
                                                        //TODO: log error not all dstores ack
                                                    }
                                                }
                                            } else {
                                                //clientOut.write(("ERROR_FILE_DOES_NOT_EXIST").getBytes(StandardCharsets.UTF_8));
                                                clientOut.println("ERROR_FILE_DOES_NOT_EXIST");
                                                clientOut.flush();
                                            }
                                        } else if (command.equals("LIST")) {
                                            StringBuilder outputMsg = new StringBuilder("LIST ");
                                            controller.index.forEach((k, v) -> outputMsg.append(v.getName()).append(" "));

                                            //clientOut.write(outputMsg.toString().getBytes(StandardCharsets.UTF_8));
                                            clientOut.println(outputMsg);
                                            System.out.println(outputMsg);
                                            //clientOut.close();
                                            clientOut.flush();

                                        } else if (command.equals("JOIN")) {
                                            //get port
                                            String port = firstBuffer.substring(firstSpace + 1, buflen);
                                            System.out.println("new dstore joined on port " + port);

                                            controller.dStores.add(new DStoreObject(Integer.parseInt(port), client));

                                            //controller.rebalance();
                                            //TODO: re-enable rebalance once other systems are tested
                                        } else {
                                            //malformed command
                                            //TODO: log error and continue
                                        }
                                    } catch (Exception e) {
                                        System.out.println(e);
                                    }
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
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

            if (command.equals("LIST")) {
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
            StringBuilder outputMsg = new StringBuilder("REBALANCE ");
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

            if (!command.equals("REBALANCE_COMPLETE")) {
                //TODO: log error
            }
        }
    }
}
