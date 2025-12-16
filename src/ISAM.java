import java.io.File;
import java.io.IOException;

public class ISAM {
    String indexFile =  null;
    String recordsFile = null;
    String overflowFile = null;

    public double alfa = 0.5;
    public int pageSize = 0;

    Index index;
    TRecords records;


    public ISAM(String indexFile, String recordsFile, String overflowFile, int pageSize) throws IOException {
        this.indexFile = indexFile;
        this.recordsFile = recordsFile;
        this.overflowFile = overflowFile;
        cleanup();
        this.pageSize = pageSize;
        index = new Index(indexFile, pageSize);
        records = new TRecords(recordsFile, overflowFile, pageSize);
    }

    public TRecord getRecord(int key) throws IOException {
        int pageNum = index.lookUpPageFor(key);
        if (pageNum == -1) {
            return null;
        }
        return records.getRecord(key, pageNum);
    }

    public int insert(TRecord record) throws IOException {
        if (record.key < 0) {
            throw new IllegalArgumentException("Invalid key (negative)");
        }

        if (index.pageAmount == 0) {
            records.insert(record, 0);
            IndexRecord ir = new IndexRecord(record.key, 0);
            index.insert(ir);
            index.smallestKey = ir.key;
            return 0;
        }

        if (record.key < index.smallestKey) {
            records.insert(record, 0);
            index.updateSmallestKey(record.key);
            return 0;
        }

        TRecord r = getRecord(record.key);
        if (r != null) {
            throw new IllegalStateException("Duplicate key");
        }
        int pageNum = index.getInsertPageFor(record.key);
        int result = records.insert(record, pageNum);
        if (result == 3) {
            // the key was smaller than the smallest one in records (needs index update)
            index.updateSmallestKey(record.key);
        }
        if (result == 4) {
            IndexRecord ir = new IndexRecord(record.key, records.pageAmount - 1);
            index.insert(ir);
        }
        return 0;
    }

    public int recordAmount() {
        return index.fileInsertedAmount + records.fileInsertedAmount + records.overflow.fileInsertedAmount;
    }

    // TODO: REMOVE (DEBUG)
    public void flush() throws IOException {
        index.writeCachedPage();
        records.writeCachedPage();
        records.overflow.writeCachedPage();
    }

    public void cleanup() throws IOException {
        File f = new File(this.indexFile);
        f.delete();
        f = new File(this.recordsFile);
        f.delete();
        f = new File(this.overflowFile);
        f.delete();

        index = new Index(indexFile, pageSize);
        records = new TRecords(recordsFile, overflowFile, pageSize);
    }

    public void print() throws IOException {
        System.out.println();
        index.print();
        records.print();
        System.out.println("\n[S] Total records: " + recordAmount());

    }

    public void printInSequence() {

    }
}
