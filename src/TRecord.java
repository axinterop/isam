import java.io.*;

public class TRecord extends Record {
    public static class NextRecordPos {
        int pagePos;
        int pageNum;

        public NextRecordPos() {
            pagePos = -1;
            pageNum = -1;
        }

        public NextRecordPos(int pagePos, int pageNum) {
            this.pagePos = pagePos;
            this.pageNum = pageNum;
        }

        public boolean exists() {
            return pagePos != -1 && pageNum != -1;
        }
        public void reset() {
            pagePos = -1;
            pageNum = -1;
        }
    }

    // Metadata
//    boolean deleted;

    // Data
    double a, b, h;
    NextRecordPos next = new NextRecordPos();

    public TRecord() {
        deleted = false;
        key = -1;
        a = -1;
        b = -1;
        h = -1;
    }

    public TRecord(int key, double a, double b, double h) {
        deleted = false;
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
        dos.writeInt(next.pageNum);
        dos.writeInt(next.pagePos);
    }

    @Override
    public void deserializeData(DataInputStream dis) throws IOException {
        deleted = dis.readBoolean();
        key = dis.readInt();
        a = dis.readDouble();
        b = dis.readDouble();
        h = dis.readDouble();
        next.pageNum = dis.readInt();
        next.pagePos = dis.readInt();

    }

    @Override
    public int getSizeBytes() {
        return 1 + // deleted
            Integer.BYTES * 3 // key, nextRecordPageNum, nextRecordPagePos
            + Double.BYTES * 3; // a, b, h
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (deleted) {
            sb.append("[X] ");
        }
        sb.append("[")
            .append(key == -1 ? "-" : key)
            .append("|");
        if (next.exists()) {
            sb.append(next.pageNum);
            sb.append(":");
            sb.append(next.pagePos);
        } else {
            sb.append("-");
        }
        sb.append("]");
        sb.append(
            String.format("\t\t\t\t\t(a=%.1f b=%.1f h=%.1f)", a, b, h)
        );
        return sb.toString();
    }


}
