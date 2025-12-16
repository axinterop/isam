import java.io.*;

public abstract class Page<T extends Record> implements IDataSerializable {
    public int pageSize;

    // Metadata
    public int pageNum;
    public int recordAmount;
    // end of Metadata ---

    T[] data;

    protected abstract T createRecordInstance();
    protected abstract T[] createRecordArray();


    public Page(int pageSize) {
        this.pageSize = pageSize;
        this.pageNum = 0;
        this.recordAmount = 0;
        this.data = createRecordArray();
    }

    @Override
    public void serializeData(DataOutputStream dos) throws IOException {
        dos.writeInt(pageNum);
        dos.writeInt(recordAmount);
        serializeBody(dos);
    }

    @Override
    public void deserializeData(DataInputStream dis) throws IOException {
        pageNum = dis.readInt();
        if (pageNum == -1) pageNum = 0;
        recordAmount = dis.readInt();
        if (recordAmount == -1) recordAmount = 0;
        deserializeBody(dis);
    }

    protected abstract void serializeBody(DataOutputStream dos) throws IOException;
    protected abstract void deserializeBody(DataInputStream dis) throws IOException;

    protected abstract int insert(T record);
    protected abstract void deleteRecord(int key);
    protected abstract void updateRecord(int key);
    protected abstract T getRecord(int key);

    protected T getRecordFromPos(int pos) {
        if  (pos < 0 || pos >= data.length) {
            return null;
        }
        return data[pos];
    }

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
        if (recordAmount > pageSize) {
            throw new IllegalStateException("Amount of records is bigger than page size");
        }
        return recordAmount == pageSize;
    }

    public int getLastFreeSlot() {
        if (isFull()) {
            throw new IllegalStateException("Cannot get last free slot (page is full)");
        }
        return recordAmount;
    }

    public void print(boolean cached) {
        for (int i = 0; i < pageSize; i++) {
            T r = data[i];
            System.out.print("[" + pageNum + ":");
            System.out.print(i + "] >> ");
            System.out.println(r);
        }
    }

    public void print() {
        for (int i = 0; i < pageSize; i++) {
            T r = data[i];
            System.out.print("[" + pageNum + ":");
            System.out.print(i + "] ");
            System.out.println(r);
        }
    }

}
