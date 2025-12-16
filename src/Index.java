import java.io.FileNotFoundException;
import java.io.IOException;

public class Index extends PagedFile<IndexPage> {

    int smallestKey = -1;

    public Index(String filename, int pageSize) throws FileNotFoundException {
        super(filename, pageSize);
    }

    @Override
    protected IndexPage createPageInstance() {
        return new IndexPage(pageSize);
    }

    public int insert(IndexRecord indexRecord) throws IOException {
        IndexPage lastNonEmptyPage = getLastNonFullPage();
        fileInsertedAmount++;
        return lastNonEmptyPage.insert(indexRecord);
    }

    // Linear lookup in the index
    public int lookUpPageFor(int key) throws IOException {
        int lastPageNum = 0;
        int prevKey = 0;
        int currPage = 0;
        while (currPage < pageAmount) {
            IndexPage p = readPage(currPage);
            if (p == null) {
                return lastPageNum;
            }

            for (int i = 0; i < pageSize; i++) {
                if (prevKey > p.data[i].key) {
                    return lastPageNum;
                }
                if (p.data[i].key < key) {
                    lastPageNum = p.data[i].pageNum;
                }
                if (p.data[i].key > key) {
                    if (i == pageSize - 1) {
                        currPage++;
                        continue;
                    }
                    return lastPageNum;
                }
                if (p.data[i].key == key) {
                    return p.data[i].pageNum;
                }
                prevKey = p.data[i].key;
            }
            currPage++;
        }
        return lastPageNum;
    }

    public int getInsertPageFor(int key)  throws IOException {
        int r = lookUpPageFor(key);
        if (r == -1) return 0;
        return r;
    }

    public void updateSmallestKey(int key) throws IOException {
        IndexPage ip = getPage(0);
        ip.data[0].key = key;
        smallestKey = key;
    }
}
