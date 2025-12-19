import java.io.FileNotFoundException;
import java.io.IOException;

public class Overflow extends PagedFile<TRecordPage> {
    public Overflow(String filename, int pageSize) throws FileNotFoundException {
        super(filename, pageSize);
    }

    @Override
    protected TRecordPage createPageInstance() {
        return new TRecordPage(pageSize);
    }

    public TRecord findRecord(TRecord mainRecord, int key) throws IOException {
        TRecord prev = mainRecord;
        while (true) {
            if (!prev.next.exists()) {
                return null;
            }
            TRecord curr = getRecordFromOverflow(prev.next);
            if (curr.key == key) {
                return curr;
            }
            prev = curr;
        }
    }

    public void insertToExistingLL(TRecord mainRecord, TRecord toInsert) throws IOException {
        TRecord prev = mainRecord;
        int prevCachedPageNum = cachedPage.pageNum;
        while (true) {
            TRecord curr = getRecordFromOverflow(prev.next);
            if (curr != null) {
                if (prev.key < toInsert.key && toInsert.key <= curr.key) {
                    toInsert.next = prev.next;
                    // here .insert() changes cachedPage, so `prev` loses its reference
                    // to the record that has been in cachedPage just before .insert()
                    prev.next = getLastAvailablePos();
                    if (mainRecord.key != prev.key && prev.next.pageNum != prevCachedPageNum) {
                        // TODO: FIX INSERTION
                        // newly inserted record is on different page, now there is new cachedPage,
                        // `prev`'s old state is written to disk.
                        // we need to retrieve it and update with new pointer
                        getPage(prevCachedPageNum); // is in cache
                        cachedPage.updateHard(prev.key, prev);
                    }
                    insert(toInsert);
                    break;
                }
            }
            if (curr == null) {
                prev.next = getLastAvailablePos();
                insert(toInsert);
                break;
            }
            prev = curr;
            prevCachedPageNum = cachedPage.pageNum;
        }
    }

    public void insertToNewLL(TRecord mainRecord, TRecord toInsert) throws IOException {
        mainRecord.next = getLastAvailablePos();
        insert(toInsert);
    }

    public int insert(TRecord record) throws IOException {
        TRecordPage trp = getLastNonFullPage();
        trp.insert(record);
        fileInsertedAmount++;
        return 0;
    }


    public TRecord getRecordFromOverflow(TRecord.NextRecordPos next) throws IOException {
        if (!next.exists()) return null;
        TRecordPage op = getPage(next.pageNum);
        return op.getRecordFromPos(next.pagePos);
    }

    public TRecord.NextRecordPos getLastAvailablePos() {
        TRecord.NextRecordPos nrp = new TRecord.NextRecordPos();
        nrp.pageNum = fileInsertedAmount / pageSize;
        nrp.pagePos = fileInsertedAmount % pageSize;
        return nrp;
    }
}
