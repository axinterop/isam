import java.io.FileNotFoundException;
import java.io.IOException;

public class Index extends PagedFile<IndexPage> {

    public Index(String filename, int pageSize) throws FileNotFoundException {
        super(filename, pageSize);
    }

    @Override
    protected IndexPage createPageInstance() {
        return new IndexPage(pageSize);
    }

    public int getPageFor(int key) throws IOException {
        int currentPage = 0;
        while (true) {
            IndexPage p = readPage(currentPage);
            if (p == null) {
                return -1;
            }
            for (int i = 0; i < pageSize; i++) {
                if (p.data[i].key == key) {
                    return p.data[i].pageNum;
                }
            }
        }
    }

    public int getInsertPageFor(int key) throws IOException {
        int lastInsertPage = 0;
        int currentInsertPage = 0;
        while (true) {
            IndexPage p = readPage(currentInsertPage);
            if (p == null) {
                return lastInsertPage;
            }
            if (p.data[0].key <= key) {
                lastInsertPage = currentInsertPage;
            }
            if (p.data[0].key > key) {
                return lastInsertPage;
            }
            currentInsertPage++;
        }
    }


}
