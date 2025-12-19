import java.io.File;
import java.io.IOException;

public class ISAM {
    String indexFile = null;
    String recordsFile = null;
    String overflowFile = null;

    public double alfa = 0.5;
    public int pageSize = 0;

    Index index;
    TRecords records;

    IOStats beforeLastOperation = new IOStats();


    public ISAM(String indexFile, String recordsFile, String overflowFile, int pageSize) throws IOException {
        this.indexFile = indexFile;
        this.recordsFile = recordsFile;
        this.overflowFile = overflowFile;
        cleanup();
        this.pageSize = pageSize;
        index = new Index(indexFile, pageSize);
        records = new TRecords(recordsFile, overflowFile, pageSize);
    }

    public TRecord get(int key) throws IOException {
        beforeLastOperation = getStats();

        int pageNum = index.lookUpPageFor(key);
        if (pageNum == -1) {
            return null;
        }
        return records.getRecord(key, pageNum);
    }

    private TRecord _get(int key) throws IOException {
        int pageNum = index.lookUpPageFor(key);
        if (pageNum == -1) {
            return null;
        }
        return records.getRecord(key, pageNum);
    }

    public int insert(TRecord record) throws IOException {
        beforeLastOperation = getStats();

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

        TRecord r = _get(record.key);
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
        beforeLastOperation = getStats();

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
        beforeLastOperation = getStats();
    }

    public void print() throws IOException {
        System.out.println();
        index.print();
        records.print();
        System.out.println("\n[S] Total records: " + recordAmount());
        printStatsSinceLastOp();
    }

    public class IOStats {
        int indexReads;
        int indexWrites;
        int recordsReads;
        int recordsWrites;
        int overflowReads;
        int overflowWrites;

        public int totalReads() {
            return indexReads + recordsReads + overflowReads;
        }

        public int totalWrites() {
            return indexWrites + recordsWrites + overflowWrites;
        }
    }

    public IOStats getStats() {
        IOStats io = new IOStats();
        io.indexReads = index.pageReadCount;
        io.indexWrites = index.pageWriteCount;
        io.recordsReads = records.pageReadCount;
        io.recordsWrites = records.pageWriteCount;
        io.overflowReads = records.overflow.pageReadCount;
        io.overflowWrites = records.overflow.pageWriteCount;
        return io;
    }

    public IOStats getStatsSinceLastOp() {
        IOStats io = new IOStats();
        io.indexReads = index.pageReadCount - beforeLastOperation.indexReads;
        io.indexWrites = index.pageWriteCount - beforeLastOperation.indexWrites;
        io.recordsReads = records.pageReadCount - beforeLastOperation.recordsReads;
        io.recordsWrites = records.pageWriteCount - beforeLastOperation.recordsWrites;
        io.overflowReads = records.overflow.pageReadCount - beforeLastOperation.overflowReads;
        io.overflowWrites = records.overflow.pageWriteCount - beforeLastOperation.overflowWrites;
        return io;
    }

    public void printInSequenceRW() throws IOException {
        beforeLastOperation = getStats();

        for (int pi = 0; pi < records.pageAmount; pi++) {
            TRecordPage p = records.getPage(pi);
            for (int ri = 0; ri < p.recordAmount; ri++) {
                TRecord r = p.getRecordFromPos(ri);
                System.out.print(r.key + " ");
                while (r.next.exists()) {
                    r = records.overflow.getRecordFromOverflow(r.next);
                    System.out.print(r.key + " ");
                }
            }
        }
        System.out.println();
    }

    public void printStatsSinceLastOp() {
        IOStats io = getStatsSinceLastOp();
        if (io.totalReads() != 0 || io.totalWrites() != 0) {
            System.out.println("[S] Last operation required:");
        }
        if (io.indexReads != 0) {
            System.out.println("- Index reads: " + io.indexReads);
        }
        if (io.indexWrites != 0) {
            System.out.println("- Index writes: " + io.indexWrites);
        }
        if (io.recordsReads != 0) {
            System.out.println("- Records reads: " + io.recordsReads);
        }
        if (io.recordsWrites != 0) {
            System.out.println("- Records writes: " + io.recordsWrites);
        }
        if (io.overflowReads != 0) {
            System.out.println("- Overflow reads: " + io.overflowReads);
        }
        if (io.overflowWrites != 0) {
            System.out.println("- Overflow writes: " + io.overflowWrites);
        }
        if (io.totalReads() != 0 || io.totalWrites() != 0) {
            System.out.println("== Total reads: " + io.totalReads());
            System.out.println("== Total writes: " + io.totalWrites());
        }
    }
}
