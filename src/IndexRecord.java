import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IndexRecord extends Record {
    int key;
    int pageNum;
    public IndexRecord(int key, int pageNum) {
        this.key = key;
        this.pageNum = pageNum;
    }

    public IndexRecord() {
    }

    @Override
    public void serializeData(DataOutputStream dos) throws IOException {
        dos.writeInt(key);
        dos.writeInt(pageNum);
    }

    @Override
    public void deserializeData(DataInputStream dis) throws IOException {
        key = dis.readInt();
        pageNum = dis.readInt();
    }

    @Override
    public String toString() {
        return String.format("Index: %d:%d", key, pageNum);
    }

    @Override
    public int getSizeBytes() {
        return Integer.BYTES * 2;
    }
}
