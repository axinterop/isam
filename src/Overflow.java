import java.io.FileNotFoundException;
import java.io.IOException;

public class Overflow extends PagedFile<IndexPage> {
    public Overflow(String filename, int pageSize) throws FileNotFoundException {
        super(filename, pageSize);
    }

    @Override
    protected IndexPage createPageInstance() {
        return new IndexPage(pageSize);
    }

     public void insertToExisting(TRecord mainRecord, TRecord toInsert) {
          int nextPage = mainRecord.nextRecordPageNum;
          int nextPos  = mainRecord.nextRecordPagePos;


     }

}
