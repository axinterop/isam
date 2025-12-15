import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TRecordPage extends Page<TRecord> {

    @Override
    protected TRecord createRecordInstance() {
        return new TRecord();
    }

    @Override
    protected TRecord[] createRecordArray() {
        return new TRecord[pageSize];
    }

    public TRecordPage(int pageSize) {
        super(pageSize);
    }

    @Override
    protected void serializeBody(DataOutputStream dos) throws IOException {
        // TODO: Change to handle empty spaces
        for (TRecord record : data) {
            record.serializeData(dos);
        }
    }

    @Override
    protected void deserializeBody(DataInputStream dis) throws IOException {
        // TODO: Change to handle empty spaces
        for (int i = 0; i < pageSize; i++) {
            TRecord r = new TRecord();
            r.deserializeData(dis);
            data[i] = r;
        }
    }

    @Override
    protected int insertRecord(TRecord record)  {
        if (recordAmount < pageSize) {
            data[recordAmount] = record;
            recordAmount++;
            return 0;
        } else {
            overflow = true;
            return -1;
        }
    }

    @Override
    protected void deleteRecord(int key) {

    }

    @Override
    protected void updateRecord(int key) {

    }

    @Override
    protected void getRecord(int key)  {

    }

    @Override
    protected int getSizeBytesRest() {
        return new TRecord().getSizeBytes() * pageSize;

    }

}
