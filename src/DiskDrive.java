import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DiskDrive {

    static final String RECORDS_FILE = "records.dat";
    static final String BTREE_FILE = "btree.dat";

    private final int PAGE_SIZE = 4;
    static boolean CONTROL = false;
    static int MAX_TO_PRINT = 50;
    static boolean LIMIT_PRINT = false;

    private final int pageSize;
    private final Map<Main.File, RandomAccessFile> rafs = new HashMap<>();

    private static class FileCache {
        int pageIndex = -1;
        Record[] page = null;
        int filledPage = 0;
        boolean dirty = false;
    }

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

    private final Map<Main.File, FileCache> caches = new HashMap<>();

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

    private FileCache getCache(Main.File fileType) {
        return caches.computeIfAbsent(fileType, f -> new FileCache());
    }

    public void deleteContent(Main.File fileType) throws IOException {
        FileCache fc = getCache(fileType);
        fc.dirty = false;
        fc.page = new Record[PAGE_SIZE];
        for (int i = 0; i < PAGE_SIZE; i++) {
            fc.page[i] = null;
        }
        fc.filledPage = 0;
        fc.pageIndex = -1;
        RandomAccessFile raf = getRaf(fileType);
        raf.setLength(0);
    }

    public void printFull(Main.File fileType) throws IOException {
        this.printDisk(fileType);
        FileCache fc = getCache(fileType);
        for (Record r : fc.page) {
            System.out.println("[P] " + r);
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

    private void flushCachedPageIfDirty(Main.File fileType) throws IOException {
        FileCache fc = getCache(fileType);
        if (
            fc == null || fc.page == null || !fc.dirty || fc.pageIndex < 0
        ) return;

        RandomAccessFile raf = getRaf(fileType);
        long firstByteOfPage = (long) fc.pageIndex * pageSize * Record.sizeBytes();
        raf.seek(firstByteOfPage);

        for (int i = 0; i < pageSize; i++) {
            Record r = fc.page[i];
            if (r == null) {
                break;
            } else {
                raf.writeDouble(r.a);
                raf.writeDouble(r.b);
                raf.writeDouble(r.h);
            }
        }

        pageWriteCount++;
        fc.dirty = false;
        fc.filledPage = 0;
    }

    private void loadPageIntoCache(Main.File filetype, int pageIndex)
        throws IOException {
        FileCache fc = getCache(filetype);
        if (fc.page != null && fc.pageIndex == pageIndex) {
            return;
        }
        flushCachedPageIfDirty(filetype);

        RandomAccessFile raf = getRaf(filetype);
        long fileByteLength = raf.length();
        long firstByteOfPage = (long) pageIndex * pageSize * Record.sizeBytes();

        if (fc.page == null || fc.page.length != pageSize) {
            fc.page = new Record[pageSize];
        } else {
            Arrays.fill(fc.page, null);
        }

        // Page beyond EOF -> all nulls
        // ...

        raf.seek(firstByteOfPage);
        for (int i = 0; i < pageSize; i++) {
            long pos = firstByteOfPage + (long) i * Record.sizeBytes();
            if (pos + Record.sizeBytes() > fileByteLength) {
                fc.page[i] = null;
            } else {
                try {
                    double a = raf.readDouble();
                    double b = raf.readDouble();
                    double h = raf.readDouble();
                    Record r = new Record();
                    r.a = a;
                    r.b = b;
                    r.h = h;
                    fc.page[i] = r;
                } catch (EOFException e) {
                    fc.page[i] = null;
                }
            }
        }

        pageReadCount++;
        fc.pageIndex = pageIndex;
        fc.dirty = false;
    }

    // ---- public ----

    public Record readRecord(Main.File filetype, long recordIndex)
        throws IOException {
        if (recordIndex < 0) return null;

        int pageIndex = (int) (recordIndex / pageSize);
        int offsetInPage = (int) (recordIndex % pageSize);

        loadPageIntoCache(filetype, pageIndex);
        FileCache fc = getCache(filetype);
        if (fc.page == null) return null;
        return fc.page[offsetInPage];
    }

    public void writeRecord(Main.File filetype, Record r)
        throws IOException {
        int pageIndex = (int) (r.id / pageSize);
        int offsetInPage = (int) (r.id % pageSize);

        loadPageIntoCache(filetype, pageIndex);
        FileCache fc = getCache(filetype);
        fc.page[offsetInPage] = r;
//            fc.filledPage++;
//            if (fc.filledPage == PAGE_SIZE) {
        fc.dirty = true;
//            }
//            flushCachedPageIfDirty(file);
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
        for (FileCache fc : caches.values()) {
            fc.dirty = true;
        }
        for (Main.File filetype : caches.keySet()) {
            flushCachedPageIfDirty(filetype);
        }
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
        caches.clear();
        if (CONTROL) {
            System.out.println("[i] Disk has been closed");
        }
    }
}
