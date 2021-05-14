public class FileObject {
    private String name;
    private int size;
    private String status;

    public FileObject (String name, int size, String status)
    {
        this.name = name;
        this.size = size;
        this.status = status;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }
}