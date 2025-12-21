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

    public TRecord findRecord(TRecord rootRecord, int key) throws IOException {
        TRecord prev = rootRecord;
        while (true) {
            if (!prev.next.exists()) {
                return null;
            }
            TRecord curr = getRecordFromOverflow(prev.next);
            if (curr.key == key) {
                if (curr.deleted) {
                    return null;
                }
                return curr;
            }
            prev = curr;
        }
    }

    public void insertToExistingLL(TRecord rootRecord, TRecord toInsert) throws IOException {
        TRecord prev = rootRecord;
        int prevCachedPageNum = cachedPage.pageNum;
        while (true) {
            TRecord curr = getRecordFromOverflow(prev.next);
            if (curr != null) {
                if (prev.key < toInsert.key && toInsert.key <= curr.key) {
                    toInsert.next = prev.next;
                    // here .insert() changes cachedPage, so `prev` loses its reference
                    // to the record that has been in cachedPage just before .insert()
                    prev.next = getLastAvailablePos();
                    getPage(prevCachedPageNum);
                    cachedPage.updateHard(prev.key, prev);

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

    public void insertToNewLL(TRecord rootRecord, TRecord toInsert) throws IOException {
        rootRecord.next = getLastAvailablePos();
        insert(toInsert);
    }

    public int insert(TRecord record) throws IOException {
        TRecordPage trp = getLastNonFullPage();
        trp.insert(record);
        fileInsertedAmount++;
        return 0;
    }

    public int updateRecord(TRecord rootRecord, TRecord updated) throws IOException {
        TRecord toUpdate = findRecord(rootRecord, updated.key);
        if (toUpdate.key != updated.key) {
            throw new IOException("Key mismatch");
        }
        toUpdate.a = updated.a;
        toUpdate.b = updated.b;
        toUpdate.h = updated.h;
        return 0;
    }

    public int deleteRecord(TRecord rootRecord, int key) throws IOException {
        TRecord toDelete = findRecord(rootRecord, key);
        if (toDelete == null) {
            return -1;
        }
        toDelete.deleted = true;
        fileDeletedAmount++;
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
