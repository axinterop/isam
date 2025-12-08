import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DiskDrive {

    static final String RECORDS_FILE = "records.dat";
    static final String BTREE_FILE = "btree.dat";

    static final int BTREE_PAGE_SIZE_ORDER_2 = 56;  // For order=2: 4 + 4*8 + 5*4 = 56 bytes

    private final int PAGE_SIZE = 4;
    static boolean CONTROL = false;
    static int MAX_TO_PRINT = 50;
    static boolean LIMIT_PRINT = false;

    private final int pageSize;
    private final Map<Main.File, RandomAccessFile> rafs = new HashMap<>();

    // Record file cache
    private int recordCachePageIndex = -1;
    private Record[] recordCachePage = null;
    private boolean recordCacheDirty = false;

    // BTree file cache
    private int btreeCachePageIndex = -1;
    private byte[] btreeCachePage = null;
    private boolean btreeCacheDirty = false;

    public class DiskStats {
        public long pageReadCount = 0;
        public long pageWriteCount = 0;

        DiskStats() {};

        @Override
        public String toString() {
            return String.format(
                "pageReadCount=%d, pageWriteCount=%d",
                pageReadCount,
                pageWriteCount
            );
        }
    }

    public long pageReadCount = 0;
    public long pageWriteCount = 0;

    public DiskDrive() {
        this.pageSize = PAGE_SIZE;
    }

    public DiskDrive(int pageSize) {
        this.pageSize = pageSize;
    }

    public DiskStats getStats() {
        DiskStats ds = new DiskStats();
        ds.pageReadCount = pageReadCount;
        ds.pageWriteCount = pageWriteCount;
        return ds;
    }

    static public String getFileFromFileType(Main.File fileType) {
        return switch (fileType) {
            case BTREE ->  BTREE_FILE;
            case RECORDS ->  RECORDS_FILE;
        };
    }

    private RandomAccessFile getRaf(Main.File fileType) throws IOException {
        RandomAccessFile raf = rafs.get(fileType);
        if (raf == null) {
            raf = new RandomAccessFile(getFileFromFileType(fileType), "rw");
            rafs.put(fileType, raf);
        }
        return raf;
    }

    public void deleteContent(Main.File fileType) throws IOException {
        if (fileType == Main.File.RECORDS) {
            recordCachePage = new Record[pageSize];
            recordCachePageIndex = -1;
            recordCacheDirty = false;
        } else if (fileType == Main.File.BTREE) {
            btreeCachePage = null;
            btreeCachePageIndex = -1;
            btreeCacheDirty = false;
        }

        RandomAccessFile raf = getRaf(fileType);
        raf.setLength(0);
    }

    public void printFull(Main.File fileType) throws IOException {
        this.printDisk(fileType);
        if (fileType == Main.File.RECORDS) {
            if (recordCachePage != null) {
                for (Record r : recordCachePage) {
                    System.out.println("[P] " + r);
                }
            }
        }
    }

    public void printAll() throws IOException {
        System.out.println("==== PRINT ALL ====");
        for (Main.File filetype : rafs.keySet()) {
            printFull(filetype);
        }
        System.out.println("==== END OF PRINT ALL ====");
    }

    public void printStats() {
        System.out.println("PageRead: " + this.pageReadCount);
        System.out.println("PageWrite: " + this.pageWriteCount);
    }

    // ---- cache helpers ----

    private void flushRecordCacheIfDirty() throws IOException {
        if (!recordCacheDirty || recordCachePage == null || recordCachePageIndex < 0) {
            return;
        }

        RandomAccessFile raf = getRaf(Main.File.RECORDS);
        long firstByteOfPage = (long) recordCachePageIndex * pageSize * Record.sizeBytes();
        raf.seek(firstByteOfPage);

        for (int i = 0; i < pageSize; i++) {
            Record r = recordCachePage[i];
            if (r == null) {
                break;
            } else {
                raf.writeDouble(r.a);
                raf.writeDouble(r.b);
                raf.writeDouble(r.h);
            }
        }

        pageWriteCount++;
        recordCacheDirty = false;
    }

    private void loadRecordPageIntoCache(int pageIndex) throws IOException {
        if (recordCachePage != null && recordCachePageIndex == pageIndex) {
            return;
        }

        flushRecordCacheIfDirty();

        RandomAccessFile raf = getRaf(Main.File.RECORDS);
        long fileByteLength = raf.length();
        long firstByteOfPage = (long) pageIndex * pageSize * Record.sizeBytes();

        if (recordCachePage == null || recordCachePage.length != pageSize) {
            recordCachePage = new Record[pageSize];
        } else {
            Arrays.fill(recordCachePage, null);
        }

        raf.seek(firstByteOfPage);
        for (int i = 0; i < pageSize; i++) {
            long pos = firstByteOfPage + (long) i * Record.sizeBytes();
            if (pos + Record.sizeBytes() > fileByteLength) {
                recordCachePage[i] = null;
            } else {
                try {
                    double a = raf.readDouble();
                    double b = raf.readDouble();
                    double h = raf.readDouble();
                    Record r = new Record();
                    r.a = a;
                    r.b = b;
                    r.h = h;
                    recordCachePage[i] = r;
                } catch (EOFException e) {
                    recordCachePage[i] = null;
                }
            }
        }

        pageReadCount++;
        recordCachePageIndex = pageIndex;
        recordCacheDirty = false;
    }

    private void flushBTreeCacheIfDirty() throws IOException {
        if (!btreeCacheDirty || btreeCachePage == null || btreeCachePageIndex < 0) {
            return;
        }

        RandomAccessFile raf = getRaf(Main.File.BTREE);
        long filePosition = (long) btreeCachePageIndex * BTREE_PAGE_SIZE_ORDER_2;
        raf.seek(filePosition);
        raf.write(btreeCachePage, 0, btreeCachePage.length);

        pageWriteCount++;
        btreeCacheDirty = false;
    }

    private void loadBTreePageIntoCache(int pageNumber) throws IOException {
        if (btreeCachePage != null && btreeCachePageIndex == pageNumber) {
            return;
        }

        flushBTreeCacheIfDirty();

        RandomAccessFile raf = getRaf(Main.File.BTREE);
        long filePosition = (long) pageNumber * BTREE_PAGE_SIZE_ORDER_2;

        // Check if page is beyond file size
        if (filePosition + BTREE_PAGE_SIZE_ORDER_2 > raf.length()) {
            btreeCachePage = null;
            btreeCachePageIndex = pageNumber;
            btreeCacheDirty = false;
            return;
        }

        btreeCachePage = new byte[BTREE_PAGE_SIZE_ORDER_2];
        raf.seek(filePosition);
        int bytesRead = raf.read(btreeCachePage);

        if (bytesRead < BTREE_PAGE_SIZE_ORDER_2) {
            throw new IOException("Failed to read complete BTree page");
        }

        pageReadCount++;
        btreeCachePageIndex = pageNumber;
        btreeCacheDirty = false;
    }

    // ---- public ----

    public Record readRecord(Main.File filetype, long recordIndex) throws IOException {
        if (recordIndex < 0) return null;
        int pageIndex = (int) (recordIndex / pageSize);
        int offsetInPage = (int) (recordIndex % pageSize);
        loadRecordPageIntoCache(pageIndex);

        if (recordCachePage == null) return null;
        return recordCachePage[offsetInPage];
    }

    public void writeRecord(Main.File filetype, Record r) throws IOException {
        int pageIndex = (int) (r.id / pageSize);
        int offsetInPage = (int) (r.id % pageSize);
        loadRecordPageIntoCache(pageIndex);

        recordCachePage[offsetInPage] = r;
        recordCacheDirty = true;
    }

    /**
     * Write a BTree page (serialized as byte array) to BTREE_FILE at given page number
     */
    public void writeBTreePage(Main.File fileType, int pageNumber, byte[] pageData) throws IOException {
        if (fileType != Main.File.BTREE) {
            throw new IllegalArgumentException("writeBTreePage only supports BTREE file");
        }

        // Load into cache and mark dirty
        loadBTreePageIntoCache(pageNumber);
        btreeCachePage = pageData.clone();
        btreeCacheDirty = true;
    }

    /**
     * Read a BTree page from BTREE_FILE at given page number
     * Returns null if page doesn't exist (beyond EOF)
     */
    public byte[] readBTreePage(Main.File fileType, int pageNumber) throws IOException {
        if (fileType != Main.File.BTREE) {
            throw new IllegalArgumentException("readBTreePage only supports BTREE file");
        }

        loadBTreePageIntoCache(pageNumber);

        if (btreeCachePage == null) {
            return null;
        }

        return btreeCachePage.clone();
    }

    public void printDisk(Main.File filetype) throws IOException {
        printDisk(filetype, false);
    }

    public void printDisk(Main.File filetype, boolean toLimitPrint) throws IOException {
        try (
            DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(getFileFromFileType(filetype)))
            )
        ) {
            System.out.println("\nPRINT DISK: " + getFileFromFileType(filetype));
            int i = 1;
            while (true) {
                try {
                    System.out.println(
                        i++ + "" + ": " + Record.readFrom(in)
                    );
                    if (toLimitPrint) {
                        if (i >= MAX_TO_PRINT) {
                            System.out.println("... (max " + MAX_TO_PRINT + ")");
                            break;
                        }
                    }
                    if (LIMIT_PRINT) {
                        if (i >= MAX_TO_PRINT) {
                            System.out.println("... (max " + MAX_TO_PRINT + ")");
                            break;
                        }
                    }
                } catch (EOFException e) {
                    break;
                }
            }
        }
    }

    public void flush() throws IOException {
        flushRecordCacheIfDirty();
        flushBTreeCacheIfDirty();

        if (CONTROL) {
            System.out.println("[i] Disk has been flushed");
        }
    }

    public void close() throws IOException {
        flush();
        for (RandomAccessFile raf : rafs.values()) {
            raf.close();
        }

        rafs.clear();

        if (CONTROL) {
            System.out.println("[i] Disk has been closed");
        }
    }
}
