import javax.xml.namespace.QName;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;


public class Controller {

    private Hashtable<String, FileObject> index = new Hashtable<>();
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

                        //update index
                        if (controller.index.putIfAbsent(fileName, new FileObject(fileName, filesize, "store in progress")) != null) {
                            OutputStream clientOut = client.getOutputStream();
                            clientOut.write("ERROR_FILE_ALREADY_EXISTS".getBytes(StandardCharsets.UTF_8));
                        } else {
                            //select R dstores
                            try {
                                //build string of dstore ports
                                StringBuilder outputMsg = new StringBuilder("STORE_TO ");
                                ArrayList<DStoreObject> sublist = (ArrayList<DStoreObject>) controller.dStores.subList(0, controller.R);
                                sublist.forEach(v -> outputMsg.append(v.getPort()).append(" "));

                                try {
                                    //send the list of ports to client
                                    OutputStream clientOut = client.getOutputStream();
                                    clientOut.write(outputMsg.toString().getBytes(StandardCharsets.UTF_8));

                                    //check acks from all the dstores
                                    boolean flag = false;
                                    for (DStoreObject dstore : sublist) {
                                        InputStream dstoreIn = dstore.getSocket().getInputStream();
                                        buflen = dstoreIn.read(buf);

                                        String ack = new String(buf, 0, buflen);

                                        if (!ack.equals("ACK " + fileName)) {
                                            //no ack
                                            flag = true;
                                        }
                                        dstoreIn.close();
                                    }
                                    //change status and update client
                                    if (flag) {
                                        controller.index.remove(fileName);
                                    } else {
                                        controller.index.get(fileName).setStatus("store complete");
                                        clientOut.write("STORE_COMPLETE".getBytes(StandardCharsets.UTF_8));
                                    }
                                    clientOut.close();

                                } catch (IOException ioException) {
                                    ioException.printStackTrace();
                                }
                            } catch (IndexOutOfBoundsException e) {
                                System.out.println("not enough dstores, add more");
                                OutputStream clientOut = client.getOutputStream();
                                clientOut.write("ERROR_NOT_ENOUGH_DSTORES".getBytes(StandardCharsets.UTF_8));
                                clientOut.close();
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
                                    OutputStream clientOut = client.getOutputStream();
                                    clientOut.write(("LOAD_FROM " + dstore.getPort() + " " + controller.index.get(fileName).getSize()).getBytes(StandardCharsets.UTF_8));

                                    int count = 1;
                                    do {
                                        //check if RELOAD
                                        //find the command
                                        buflen = clientIn.read(buf);
                                        firstBuffer = new String(buf, 0, buflen);
                                        firstSpace = firstBuffer.indexOf(" ");
                                        command = firstBuffer.substring(0, firstSpace);
                                        System.out.println("command " + command);

                                        //TODO: work out if this should go in general commands to catch?
                                        if (command.equals("RELOAD")){
                                            //get file name
                                            secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                                            fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
                                            System.out.println("fileName " + fileName);

                                            try {
                                                dstore = controller.dStores.get(count);
                                            } catch (IndexOutOfBoundsException e){
                                                clientOut.write(("ERROR_LOAD").getBytes(StandardCharsets.UTF_8));
                                            }
                                            count++;
                                            clientOut.write(("LOAD_FROM " + dstore.getPort() + " " + controller.index.get(fileName).getSize()).getBytes(StandardCharsets.UTF_8));
                                        }
                                    } while (command.equals("RELOAD"));
                                    clientOut.close();

                                }  catch (Exception e) {
                                    System.out.println(e);
                                }
                            }catch (IndexOutOfBoundsException e){
                                OutputStream clientOut = client.getOutputStream();
                                clientOut.write(("ERROR_NOT_ENOUGH_DSTORES").getBytes(StandardCharsets.UTF_8));
                                clientOut.close();
                            }
                        } else {
                            OutputStream clientOut = client.getOutputStream();
                            clientOut.write(("ERROR_FILE_DOES_NOT_EXIST").getBytes(StandardCharsets.UTF_8));
                            clientOut.close();
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
                                OutputStream clientOut = client.getOutputStream();
                                clientOut.write(("ERROR_NOT_ENOUGH_DSTORES").getBytes(StandardCharsets.UTF_8));
                                clientOut.close();
                            } else {
                                for (DStoreObject dstore : controller.dStores) {
                                    OutputStream dstoreOut = dstore.getSocket().getOutputStream();
                                    dstoreOut.write(("REMOVE " + fileName).getBytes(StandardCharsets.UTF_8));

                                    InputStream dstoreIn = dstore.getSocket().getInputStream();
                                    buflen = dstoreIn.read(buf);

                                    String ack = new String(buf, 0, buflen);

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
                                    OutputStream clientOut = client.getOutputStream();
                                    clientOut.write(("REMOVE_COMPLETE").getBytes(StandardCharsets.UTF_8));
                                    clientOut.close();
                                } else {
                                    //TODO: log error not all dstores ack
                                }
                            }
                        } else {
                            OutputStream clientOut = client.getOutputStream();
                            clientOut.write(("ERROR_FILE_DOES_NOT_EXIST").getBytes(StandardCharsets.UTF_8));
                            clientOut.close();
                        }
                    } else if (command.equals("LIST")) {
                        StringBuilder outputMsg = new StringBuilder("LIST ");
                        controller.index.forEach((k,v) -> outputMsg.append(v.getName()).append(" "));

                        OutputStream clientOut = client.getOutputStream();
                        clientOut.write(outputMsg.toString().getBytes(StandardCharsets.UTF_8));
                        clientOut.close();

                    } else if (command.equals("JOIN")) {
                        //get port
                        int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                        String port = firstBuffer.substring(firstSpace + 1, secondSpace);
                        System.out.println("new dstore joined on port " + port);

                        controller.dStores.add(new DStoreObject(Integer.parseInt(port), new ServerSocket(controller.cport).accept()));

                        controller.rebalance();
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

    private void rebalance() throws IOException {
        HashMap<String, ArrayList<DStoreObject>> files = new HashMap<>();

        for (DStoreObject dstore : dStores) {
            OutputStream dstoreOut = dstore.getSocket().getOutputStream();
            dstoreOut.write(("LIST").getBytes(StandardCharsets.UTF_8));

            byte[] buf = new byte[1000];
            int buflen;

            //find the command
            InputStream dstoreIn = dstore.getSocket().getInputStream();
            buflen = dstoreIn.read(buf);
            String firstBuffer = new String(buf, 0, buflen);
            int firstSpace = firstBuffer.indexOf(" ");
            String command = firstBuffer.substring(0, firstSpace);
            System.out.println("command " + command);

            if (command.equals("LIST")) {
                String[] listOfFilesInDstore = firstBuffer.substring(firstSpace + 1, buflen).split(" ");
                Arrays.stream(listOfFilesInDstore).forEach(file -> {
                    if (files.containsKey(file)){
                        files.get(file).add(dstore);
                    } else {
                        files.put(file, new ArrayList<DStoreObject>(List.of(dstore)));
                    }
                });
            }
        }

        files.size();
        dStores.size();
        //TODO: share files.size() between all dstores, according to R value
    }
}
