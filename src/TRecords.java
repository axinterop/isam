import java.io.FileNotFoundException;
import java.io.IOException;

public class TRecords extends PagedFile<TRecordPage> {
    Overflow overflow;

    public TRecords(String filename, int pageSize) throws FileNotFoundException {
        super(filename, pageSize);
        overflow = new Overflow("overflow.dat", pageSize);
    }

    @Override
    protected TRecordPage createPageInstance() {
        return new TRecordPage(pageSize);
    }

    public int insertRecord(TRecord record, int pageNum) throws IOException {
        TRecordPage trp = readPage(pageNum);
        if (trp.insertRecord(record) == 0) return 0;

        // Find previous record
        // TRecord mainRecord = findPrevious(trp, record.key);

        // If this record has link to overflow
        // overflow.insertToExisting(mainRecord, record);

        // If this record has no link
        // overflow.insertNew(record);

        return 0;
    }

}
