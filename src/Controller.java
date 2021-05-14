import javax.xml.namespace.QName;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;


public class Controller {



    public static void main(String[] args) throws IOException {
        int cport = Integer.parseInt(args[0]);
        int R = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalance_period = Integer.parseInt(args[3]);

        HashMap<String, FileObject> index = new HashMap<>();
        ArrayList<DStoreObject> dStores = new ArrayList<>();

        //start server and keep checking for connections
        try {
            ServerSocket clientSocket = new ServerSocket(cport);

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
                        if (index.putIfAbsent(fileName, new FileObject(fileName, filesize, "store in progress")) != null) {
                            OutputStream clientOut = client.getOutputStream();
                            clientOut.write("ERROR_FILE_ALREADY_EXISTS".getBytes(StandardCharsets.UTF_8));
                        } else {
                            //select R dstores
                            try {
                                //build string of dstore ports
                                StringBuilder outputMsg = new StringBuilder("STORE_TO ");
                                ArrayList<DStoreObject> sublist = (ArrayList<DStoreObject>) dStores.subList(0, R);
                                for (DStoreObject dstore : sublist) {
                                    outputMsg.append(dstore.getPort()).append(" ");
                                }


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
                                    }
                                    //change status and update client
                                    if (flag) {
                                        index.remove(fileName);
                                    } else {
                                        index.get(fileName).setStatus("store complete");
                                        clientOut.write("STORE_COMPLETE".getBytes(StandardCharsets.UTF_8));
                                    }
                                } catch (IOException ioException) {
                                    ioException.printStackTrace();
                                }
                            } catch (IndexOutOfBoundsException e) {
                                System.out.println("not enough dstores, add more");
                                OutputStream clientOut = client.getOutputStream();
                                clientOut.write("ERROR_NOT_ENOUGH_DSTORES".getBytes(StandardCharsets.UTF_8));
                            }
                        }
                    } else if (command.equals("LOAD")) {
                        //get file name
                        int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                        String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
                        System.out.println("fileName " + fileName);

                        if (index.containsKey(fileName)) {
                            try {
                                DStoreObject dstore = dStores.get(0);

                                try {
                                    OutputStream clientOut = client.getOutputStream();
                                    clientOut.write(("LOAD_FROM " + dstore.getPort() + " " + index.get(fileName).getSize()).getBytes(StandardCharsets.UTF_8));

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
                                                dstore = dStores.get(count);
                                            } catch (IndexOutOfBoundsException e){
                                                clientOut.write(("ERROR_LOAD").getBytes(StandardCharsets.UTF_8));
                                            }
                                            count++;
                                            clientOut.write(("LOAD_FROM " + dstore.getPort() + " " + index.get(fileName).getSize()).getBytes(StandardCharsets.UTF_8));
                                        }
                                    } while (command.equals("RELOAD"));

                                }  catch (Exception e) {
                                    System.out.println(e);
                                }
                            }catch (IndexOutOfBoundsException e){
                                OutputStream clientOut = client.getOutputStream();
                                clientOut.write(("ERROR_NOT_ENOUGH_DSTORES").getBytes(StandardCharsets.UTF_8));
                            }
                        } else {
                            OutputStream clientOut = client.getOutputStream();
                            clientOut.write(("ERROR_FILE_DOES_NOT_EXIST").getBytes(StandardCharsets.UTF_8));
                        }

                    } else if (command.equals("REMOVE")) {
                        //get file name
                        int secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                        String fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
                        System.out.println("fileName " + fileName);

                        index.get(fileName).setStatus("remove in progress");

                        
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
