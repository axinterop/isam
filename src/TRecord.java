import java.io.*;

public class TRecord extends Record {

    // Metadata
    boolean deleted;

    // Data
    int key;
    double a, b, h;
    int nextRecordPageNum;
    int nextRecordPagePos;

    public TRecord() {};

    public TRecord(int key, double a, double b, double h) {
        this.key = key;
        this.a = a;
        this.b = b;
        this.h = h;
    }

    @Override
    public void serializeData(DataOutputStream dos) throws IOException {
        dos.writeBoolean(deleted);
        dos.writeInt(key);
        dos.writeDouble(a);
        dos.writeDouble(b);
        dos.writeDouble(h);
        dos.writeInt(nextRecordPageNum);
        dos.writeInt(nextRecordPagePos);
    }

    @Override
    public void deserializeData(DataInputStream dis) throws IOException {
        deleted = dis.readBoolean();
        key = dis.readInt();
        a = dis.readDouble();
        b = dis.readDouble();
        h = dis.readDouble();
        nextRecordPageNum = dis.readInt();
        nextRecordPagePos = dis.readInt();

    }

    @Override
    public String toString() {
        return String.format(
            "[D=%b] TRecord (%d): a=%.3f b=%.3f h=%.3f",
            deleted,
            key,
            a,
            b,
            h
        );
    }

    @Override
    public int getSizeBytes() {
        return 1 + // Boolean
            Integer.BYTES * 3 // key, nextRecordPageNum, nextRecordPagePos
            + Double.BYTES * 3; // a, b, h
    }
}
