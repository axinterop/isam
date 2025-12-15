import java.io.FileNotFoundException;
import java.io.IOException;

public class ISAM {
    public double alfa = 0.5;
    public double beta = 0.2;
    public int pageSize = 0;

    Index index;
    TRecords records;
    Overflow overflow;


    public ISAM(String indexFile, String recordsFile, String overflowFile, int pageSize) throws FileNotFoundException {
        this.pageSize = pageSize;
        index =     new Index(indexFile, pageSize);
        records =   new TRecords(recordsFile, pageSize);
        overflow =  new Overflow(overflowFile, pageSize);
    }

    public TRecord getRecord(int key) throws IOException {
        int pageNum = index.getPageFor(key);
        if (pageNum == -1) {
            return null;
        }
        // Next action
        return new TRecord(0, 1, 2, 3);
    }

    public int insertRecord(TRecord record) throws IOException {
        TRecord r = getRecord(record.key);
        if (r != null) {
            return -1;
        }
        int pageNum = index.getInsertPageFor(record.key);
        int result = records.insertRecord(record, pageNum);
        return 0;
    }
//
//    public void deleteRecord(Record record) {
//
//    }

    // TODO: REMOVE (DEBUG)
    public void flush() throws IOException {
        index.writeCachedPage();
    }
}
