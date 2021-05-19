import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class DStoreObject {
    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    private int port;
    private Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;

    public DStoreObject(int port, Socket socket, InputStream inputStream, OutputStream outputStream) {
        this.port = port;
        this.socket = socket;
        this.inputStream = inputStream;
        this.outputStream = outputStream;
    }

    public InputStream getInputStream() {
        return inputStream;
    }

    public OutputStream getOutputStream() {
        return outputStream;
    }
}
