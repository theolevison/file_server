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

        HashMap<String, String> index = new HashMap<>();
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
                        if (index.putIfAbsent(fileName, "store in progress") != null) {
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
                                        index.put(fileName, "store complete");
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
                                    clientOut.write(("LOAD_FROM " + dstore.getPort() + " " + fileName).getBytes(StandardCharsets.UTF_8));

                                    //check if RELOAD
                                    //find the command
                                    buflen = clientIn.read(buf);
                                    firstBuffer = new String(buf, 0, buflen);
                                    firstSpace = firstBuffer.indexOf(" ");
                                    command = firstBuffer.substring(0, firstSpace);
                                    System.out.println("command " + command);
                                    //TODO: work out how input stream works and how to make this in a while loop.

                                    if (command.equals("RELOAD")){
                                        //get file name
                                        secondSpace = firstBuffer.indexOf(" ", firstSpace + 1);
                                        fileName = firstBuffer.substring(firstSpace + 1, secondSpace);
                                        System.out.println("fileName " + fileName);
                                    }

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
