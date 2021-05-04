import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class DStore {

    public static void main(String[] args) throws IOException {
        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String file_folder= args[3];

        //start server and keep checking for connections
        try {
            ServerSocket ss = new ServerSocket(port);

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

                        //TODO: ack
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