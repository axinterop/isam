import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IndexRecord extends Record {
    int pageNum;
    public IndexRecord(int key, int pageNum) {
        this.key = key;
        this.pageNum = pageNum;
    }

    public IndexRecord() {
        this.key = -1;
        this.pageNum = -1;
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
        if (pageNum == -1 || key == -1) {
            return "IndexRecord: [-]";
        }
        return String.format("IndexRecord: [P=%d:K=%d]", pageNum, key);
    }

    @Override
    public int getSizeBytes() {
        return Integer.BYTES * 2;
    }
}
