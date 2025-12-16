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
        while (true) {
            TRecord curr = getRecordFromOverflow(prev.next);
            if (curr != null) {
                if (prev.key < toInsert.key && toInsert.key <= curr.key) {
                    toInsert.next = prev.next;
                    prev.next = insert(toInsert);
                    break;
                }
            }
            if (curr == null) {
                prev.next = insert(toInsert);
                break;
            }
            prev = curr;
        }
    }

    public void insertToNewLL(TRecord mainRecord, TRecord toInsert) throws IOException {
        mainRecord.next = insert(toInsert);
    }

    public TRecord.NextRecordPos insert(TRecord record) throws IOException {
        TRecord.NextRecordPos result = new TRecord.NextRecordPos();

        TRecordPage trp = getLastNonFullPage();
        int pos = trp.getLastFreeSlot();

        trp.insert(record);
        fileInsertedAmount++;

        result.pageNum = trp.pageNum;
        result.pagePos = pos;
        return result;
    }

    public TRecord getRecordFromOverflow(TRecord.NextRecordPos next) throws IOException {
        if (!next.exists()) return null;
        TRecordPage op = getPage(next.pageNum);
        return op.getRecordFromPos(next.pagePos);
    }
}
