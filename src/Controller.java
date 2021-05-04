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
        ArrayList<Integer> dStores = new ArrayList<>();

        //start server and keep checking for connections
        try {
            ServerSocket ss = new ServerSocket(cport);

            for (; ; ) {
                try {
                    System.out.println("waiting for connection");
                    Socket client = ss.accept();
                    System.out.println("connected");
                    InputStream in = client.getInputStream();

                    byte[] buf = new byte[1000];
                    int buflen;

                    //find the command
                    buflen = in.read(buf);
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
                        if (index.putIfAbsent(fileName, "store in progress") != null){
                            throw new Exception("Error file already exists");
                            //TODO: file exists
                        } else {
                            //select R dstores
                            try {
                                //build string of dstore ports
                                StringBuilder outputMsg = new StringBuilder("STORE_TO ");
                                for (int dstore : dStores.subList(0,R)) {
                                    outputMsg.append(dstore).append(" ");
                                }

                                //send the list of ports to client
                                try {
                                    OutputStream out = client.getOutputStream();
                                    out.write(outputMsg.toString().getBytes(StandardCharsets.UTF_8));
                                    //TODO: check acks from all the dstores
                                } catch (IOException ioException) {
                                    ioException.printStackTrace();
                                }
                            } catch (IndexOutOfBoundsException e) {
                                System.out.println("not enough dstores, add more");
                                //TODO: more dstores
                            }
                        }
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
