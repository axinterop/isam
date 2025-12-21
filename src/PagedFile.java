import java.io.*;
import java.util.Arrays;

public abstract class PagedFile<T extends Page> {
    public String filename;
    public RandomAccessFile raf;
    public int pageSize;

    public int pageReadCount;
    public int pageWriteCount;

    public T cachedPage;

    int pageAmount;
    int fileInsertedAmount;
    int fileDeletedAmount;

    public PagedFile(String filename, int pageSize) throws FileNotFoundException {
        this.filename = filename;
        raf = new RandomAccessFile(filename, "rw");

        this.pageSize = pageSize;

        this.pageReadCount = 0;
        this.pageWriteCount = 0;
        this.cachedPage = null;

        this.pageAmount = 0; // TODO: If we want to save all states, this should be changed
    }

    protected abstract T createPageInstance();

    public T getPage(int n) throws IOException {
        T page = readPage(n);
        if (page == null) {
            return getNewPage();
        }
        return page;
    }

    public T readPage(int n) throws IOException {
        if (cachedPage != null && n == cachedPage.pageNum) {
            return cachedPage;
        }
        writeCachedPage();

        T p = createPageInstance();

        byte[] b = new byte[p.getSizeBytes()];
        Arrays.fill(b, (byte) -1);
        if (n <= pageAmount) {
            raf.seek((long) p.getSizeBytes() * n);
        } else {
            raf.seek((long) p.getSizeBytes() * pageAmount);
        }
        int read = raf.read(b);
        if (read == -1) return null;

        p.deserialize(b);
        cachedPage = p;

        pageReadCount++;
        return p;
    }

    public void writeCachedPage() throws IOException {
        if (cachedPage == null) {
           return;
        }

        byte[] b = cachedPage.serialize();
        raf.seek((long) cachedPage.getSizeBytes() * cachedPage.pageNum);
        raf.write(b);

        pageWriteCount++;
    }

    public T getNewPage() throws IOException {
        writeCachedPage();

        T page = createPageInstance();
        byte[] b = new byte[page.getSizeBytes()];
        Arrays.fill(b, (byte) -1);
        page.deserialize(b);

        page.pageNum = pageAmount;

        pageAmount++;
        cachedPage = page;
        return page;
    }

    public T getLastNonFullPage() throws IOException {
        T page = getLastPage();
        if (page.isFull()) return getNewPage();
        return page;
    }

    public T getLastPage() throws IOException {
        if (pageAmount == 0) return getNewPage();
        return getPage(pageAmount - 1);
    }

    public void print() throws IOException {
        System.out.println("    " + filename);
        System.out.println("- Inserted records: " + fileInsertedAmount);
        System.out.println("- Deleted records:  " + fileDeletedAmount);
        T p = createPageInstance();
        for (int n = 0; n < pageAmount; n++) {
            if (n == cachedPage.pageNum) {
                cachedPage.print(true);
                continue;
            }

            byte[] b = new byte[p.getSizeBytes()];
            Arrays.fill(b, (byte) -1);
            raf.seek((long) p.getSizeBytes() * n);
            int read = raf.read(b);
            p.deserialize(b);
            p.print();
        }
    }
}

