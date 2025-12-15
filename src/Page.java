import java.io.*;

public abstract class Page<T extends Record> implements IDataSerializable {
    public int pageSize;

    // Metadata
    public boolean overflow;
    public int pageNum;
    public int recordAmount;
    // end of Metadata ---

    T[] data;

    protected abstract T createRecordInstance();
    protected abstract T[] createRecordArray();


    public Page(int pageSize) {
        this.overflow = false;
        this.pageSize = pageSize;
        this.pageNum = 0;
        this.recordAmount = 0;
        this.data = createRecordArray();
    }

    @Override
    public void serializeData(DataOutputStream dos) throws IOException {
        dos.writeBoolean(this.overflow);
        dos.writeInt(pageNum);
        dos.writeInt(recordAmount);
        serializeBody(dos);
    }

    @Override
    public void deserializeData(DataInputStream dis) throws IOException {
        overflow = dis.readBoolean();
        pageNum = dis.readInt();
        recordAmount = dis.readInt();
        deserializeBody(dis);
    }

    protected abstract void serializeBody(DataOutputStream dos) throws IOException;
    protected abstract void deserializeBody(DataInputStream dis) throws IOException;

    protected abstract int insertRecord(T record);
    protected abstract void deleteRecord(int key);
    protected abstract void updateRecord(int key);
    protected abstract void getRecord(int key);

    public int getSizeBytes() {
        int size = 0;
        size += Integer.BYTES; // int pageNum
        size += Integer.BYTES; // int recordAmount
        size += getSizeBytesRest();
        return size;
    }

    protected abstract int getSizeBytesRest();

    public boolean isEmpty() {
        return recordAmount == 0;
    }

    public boolean isFull() {
        return recordAmount == data.length;
    }

    public boolean isOverflow() {
        return overflow;
    }
}
