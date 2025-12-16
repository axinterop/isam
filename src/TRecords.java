import java.io.FileNotFoundException;
import java.io.IOException;

public class TRecords extends PagedFile<TRecordPage> {
    Overflow overflow;

    public TRecords(String filename, String overflow, int pageSize) throws FileNotFoundException {
        super(filename, pageSize);
        this.overflow = new Overflow(overflow, pageSize);
    }

    @Override
    protected TRecordPage createPageInstance() {
        return new TRecordPage(pageSize);
    }

    // Returns:
    // - 0 if no overflow occurred
    // - 1 if overflow occurred, but overflow linked list was created
    // - 2 if overflow occurred and `record` was inserted to existing overflow linked list
    // - 3 if the key was smaller than the smallest one in records (needs index update)
    // - 4 if record was inserted into a new page (requires index insert)
    public int insert(TRecord recordToInsert, int pageNum) throws IOException {
        TRecordPage trp = getPage(pageNum);

        if (pageNum == 0 && recordToInsert.key < trp.data[0].key) {
            TRecord recordToTransfer = trp.data[0];
            recordToInsert.next = recordToTransfer.next;
            recordToTransfer.next = new TRecord.NextRecordPos();
            trp.data[0] = recordToInsert;
            if (recordToInsert.next.exists()) {
                overflow.insertToExistingLL(recordToInsert, recordToTransfer);
            } else {
                overflow.insertToNewLL(recordToInsert, recordToTransfer);
            }
            return 3;
        }

        if (!trp.isFull()) {
            trp.insertAndSort(recordToInsert);
            fileInsertedAmount++;
            return 0;
        }

        // Find previous record
        TRecord mainRecord = trp.findPrevious(recordToInsert.key);

        // After previous condition we know, that current page is full
        // If current page is also the last page and previousRecord is at last pos
        // - we do not add to overflow but add to new page
        if (trp.pageNum == (pageAmount - 1) && trp.data[pageSize - 1].key == mainRecord.key) {
            TRecordPage newPage = getNewPage();
            newPage.insertAndSort(recordToInsert);
            fileInsertedAmount++;
            return 4;
        }

        // If this record has link to overflow
        if (mainRecord.next.exists()) {
            overflow.insertToExistingLL(mainRecord, recordToInsert);
            return 1;
        }
        // If this record has no link
        overflow.insertToNewLL(mainRecord, recordToInsert);
        return 2;
    }

    public TRecord getRecord(int key, int pageNum) throws IOException {
        TRecordPage trp = getPage(pageNum);

        TRecord onPageRecord = trp.getRecord(key);
        if (onPageRecord != null) {
            return onPageRecord;
        }
        if (trp.isOverflown()) {
            TRecord mainRecord = trp.findPrevious(key);
            return overflow.findRecord(mainRecord, key);
        }
        return null;
    }

    @Override
    public void print() throws IOException {
        super.print();
        overflow.print();
    }
}
