import java.io.*;

public abstract class PagedFile<T extends Page> {
    public String filename;
    public RandomAccessFile raf;
    public int pageSize;

    public int pageReadCount;
    public int pageWriteCount;

    public T cachedPage;

    int lastPageIndex;

    public PagedFile(String filename, int pageSize) throws FileNotFoundException {
        this.filename = filename;
        raf = new RandomAccessFile(filename, "rw");

        this.pageSize = pageSize;

        this.pageReadCount = 0;
        this.pageWriteCount = 0;
        this.cachedPage = null;

        this.lastPageIndex = -1; // TODO: If we want to save all states, this should be changed
    }

    protected abstract T createPageInstance();

    public T readPage(int n) throws IOException {
        if (cachedPage != null) {
            if (n == cachedPage.pageNum) {
                return cachedPage;
            } else {
                writeCachedPage();
            }
        }

        T p = createPageInstance();

        byte[] b = new byte[p.getSizeBytes()];
        if (n <= lastPageIndex) {
            raf.seek((long) p.getSizeBytes() * n);
        } else {
            raf.seek((long) p.getSizeBytes() * lastPageIndex);
        }
        int read = raf.read(b);
        if (read == -1) return null;

        p.deserialize(b);
        cachedPage = p;

        pageReadCount++;
        lastPageIndex++;
        return p;
    }

    public void writeCachedPage() throws IOException {
        if (cachedPage == null) {
            throw new IOException("Cached page is null");
        }

        byte[] b = cachedPage.serialize();
        raf.seek((long) pageSize * cachedPage.pageNum);
        raf.write(b);

        pageWriteCount++;
    }

    public T getLastPage() throws IOException {
        return readPage(lastPageIndex);
    }

}

