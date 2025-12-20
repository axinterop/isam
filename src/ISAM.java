import java.io.File;
import java.io.IOException;

public class ISAM {
    String indexFile = null;
    String recordsFile = null;
    String overflowFile = null;

    public int pageSize = 0;

    Index index;
    TRecords records;

    IOStats beforeLastOperation = new IOStats();

    public final double overflowThreshold = 0.5; // 50%
    public final double deletionThreshold = 0.2; // 20%
    public boolean autoReorganization = false;

    private String tempIndexFile = "temp_index.dat";
    private String tempRecordFile = "temp_record.dat";

    public ISAM(String indexFile, String recordsFile, String overflowFile, int pageSize) throws IOException {
        this.indexFile = indexFile;
        this.recordsFile = recordsFile;
        this.overflowFile = overflowFile;
        cleanupFull();
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
        if (autoReorganization) reorganize();

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

    public int delete(int key) throws IOException {
        if (autoReorganization) reorganize();

        int pageNum = index.lookUpPageFor(key);
        if (pageNum == -1) {
            return -1;
        }
        return records.deleteRecord(key, pageNum);
    }

    public int insertedRecordAmount() {
        return records.fileInsertedAmount + records.overflow.fileInsertedAmount;
    }

    public int deletedRecordAmount() {
        return records.fileDeletedAmount + records.overflow.fileDeletedAmount;
    }

    public int reorganize() throws IOException {
        if (!needsReorganization()) {
            return -1;
        }

        Index newIndex = new Index(tempIndexFile, pageSize);
        TRecords newTRecords = new TRecords(tempRecordFile, overflowFile, pageSize); // TODO: overflowFile?

        beforeLastOperation = getStats();

        TRecord.NextRecordPos rememberedPos = new TRecord.NextRecordPos();

        int newInserted = 0;
        for (int pi = 0; pi < records.pageAmount; pi++) {
            TRecordPage p = records.getPage(pi);
            for (int ri = 0; ri < p.recordAmount; ri++) {
                TRecord r = p.getRecordFromPos(ri);
                if (r.deleted) {
                    continue;
                }

                int insertPageNum = newInserted / pageSize;
                rememberedPos.pagePos = r.next.pagePos;
                rememberedPos.pageNum = r.next.pageNum;
                r.next.reset();

                int result = newTRecords.insert(r, insertPageNum);
                if (result != 0 && result != 3) throw new IllegalStateException("Insertion required additional calls.");

                if (newInserted % pageSize == 0) {
                    IndexRecord ir = new IndexRecord(r.key, insertPageNum);
                    newIndex.insert(ir);
                }

                newInserted++;

                while (rememberedPos.exists()) {
                    r = records.overflow.getRecordFromOverflow(rememberedPos);
                    rememberedPos.pagePos = r.next.pagePos;
                    rememberedPos.pageNum = r.next.pageNum;
                    if (r.deleted) {
                        continue;
                    }

                    insertPageNum = newInserted / pageSize;
                    r.next.reset();

                    result = newTRecords.insert(r, insertPageNum);
                    if (result != 0 && result != 3) throw new IllegalStateException("Insertion required additional calls.");

                    if (newInserted % pageSize == 0) {
                        IndexRecord ir = new IndexRecord(r.key, insertPageNum);
                        newIndex.insert(ir);
                    }
                    newInserted++;
                }
            }
        }

        IOStats afterReorganization = getStats();

//        newIndex.print();
//        newTRecords.print();

        File f = new File(this.indexFile);
        f.delete();
        f = new File(this.recordsFile);
        f.delete();
        f = new File(this.overflowFile);
        f.delete();

        f = new File(this.tempIndexFile);
        f.renameTo(new File(this.indexFile));
        f = new File(this.tempRecordFile);
        f.renameTo(new File(this.recordsFile));

        index = newIndex;
        records = newTRecords;


        index.filename = this.indexFile;
        records.filename = this.recordsFile;
        index.pageReadCount += afterReorganization.indexReads;
        index.pageWriteCount += afterReorganization.indexWrites;
        records.pageReadCount += afterReorganization.recordsReads;
        records.pageWriteCount += afterReorganization.recordsWrites;
        records.overflow.pageReadCount += afterReorganization.overflowReads;
        records.overflow.pageWriteCount += afterReorganization.overflowWrites;

        return 0;
    }

    public boolean needsReorganization() {
        return overflowReachedThreshold() || deletionReachedThreshold();
    }

    public boolean overflowReachedThreshold() {
        return currentOverflowRatio() >= overflowThreshold;
    }


    public double currentOverflowRatio() {
        if (records.overflow.fileInsertedAmount == 0) {
            return 0;
        }
        return (double) records.overflow.fileInsertedAmount / insertedRecordAmount();
    }

    public boolean deletionReachedThreshold() {
        return currentDeletionRatio() >= deletionThreshold;
    }

    public double currentDeletionRatio() {
        if (insertedRecordAmount() == 0) {
            return 0;
        }
        return (double) deletedRecordAmount() / insertedRecordAmount();
    }

    // TODO: REMOVE (DEBUG)
    public void flush() throws IOException {
        beforeLastOperation = getStats();

        index.writeCachedPage();
        records.writeCachedPage();
        records.overflow.writeCachedPage();
    }

    public void cleanupFull() throws IOException {
        File f = new File(this.indexFile);
        f.delete();
        f = new File(this.recordsFile);
        f.delete();
        f = new File(this.overflowFile);
        f.delete();
        cleanupReorganization();

        index = new Index(indexFile, pageSize);
        records = new TRecords(recordsFile, overflowFile, pageSize);
        beforeLastOperation = getStats();
    }

    public void cleanupReorganization() {
        File f = new File(this.tempIndexFile);
        f.delete();
        f = new File(this.tempRecordFile);
        f.delete();
    }

    public void print() throws IOException {
        System.out.println();
        index.print();
        records.print();
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

        public void print() {
            System.out.println("IO stats:");
            System.out.println("- Index reads:      " + this.indexReads);
            System.out.println("- Index writes:     " + this.indexWrites);
            System.out.println("- Records reads:    " + this.recordsReads);
            System.out.println("- Records writes:   " + this.recordsWrites);
            System.out.println("- Overflow reads:   " + this.overflowReads);
            System.out.println("- Overflow writes:  " + this.overflowWrites);
            System.out.println("== Total reads:     " + this.totalReads());
            System.out.println("== Total writes:    " + this.totalWrites());
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

    // TODO: printInSequenceRW influences RW counters, change it
    public void printInSequenceRW(boolean showDeleted) throws IOException {
        beforeLastOperation = getStats();

        for (int pi = 0; pi < records.pageAmount; pi++) {
            TRecordPage p = records.getPage(pi);
            for (int ri = 0; ri < p.recordAmount; ri++) {
                TRecord r = p.getRecordFromPos(ri);
                if (r.deleted) {
                    if (showDeleted) {
                        System.out.print(r.key + "_ ");
                    }
                } else {
                    System.out.print(r.key + " ");
                }
                while (r.next.exists()) {
                    r = records.overflow.getRecordFromOverflow(r.next);
                    if (r.deleted) {
                        if (showDeleted) {
                            System.out.print(r.key + "_ ");
                        }
                    } else {
                        System.out.print(r.key + " ");
                    }
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
            System.out.println("- Index reads:      " + io.indexReads);
        }
        if (io.indexWrites != 0) {
            System.out.println("- Index writes:     " + io.indexWrites);
        }
        if (io.recordsReads != 0) {
            System.out.println("- Records reads:    " + io.recordsReads);
        }
        if (io.recordsWrites != 0) {
            System.out.println("- Records writes:   " + io.recordsWrites);
        }
        if (io.overflowReads != 0) {
            System.out.println("- Overflow reads:   " + io.overflowReads);
        }
        if (io.overflowWrites != 0) {
            System.out.println("- Overflow writes:  " + io.overflowWrites);
        }
        if (io.totalReads() != 0 || io.totalWrites() != 0) {
            System.out.println("== Total reads:     " + io.totalReads());
            System.out.println("== Total writes:    " + io.totalWrites());
        }
    }

    public void printStats() {
        System.out.println("\tStats: ");
        System.out.println("Configuration stats:");
        System.out.println("- Current overflow ratio:           " + currentOverflowRatio() * 100 + "%");
        System.out.println("- Overflow threshold:               " + overflowThreshold * 100 + "%");
        System.out.println("- Current deletion ratio:           " + currentDeletionRatio() * 100 + "%");
        System.out.println("- Deletion threshold:               " + deletionThreshold * 100 + "%");
        System.out.println("- Is auto reorganization enabled:   " + autoReorganization);
        System.out.println("Record stats:");
        System.out.println("- Inserted records: " + insertedRecordAmount());
        System.out.println("- Deleted records:  " + deletedRecordAmount());
        printIOStats();
    }

    public void printIOStats() {
        getStats().print();
    }

}
