import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

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
    protected int insert(TRecord record) {
        if (isFull()) {
            throw new IllegalStateException("Trying to insert TRecord to non-available page");
        }
        data[recordAmount] = record;
        recordAmount++;
        return 0;
    }

    protected int insertAndSort(TRecord record) {
        insert(record);
        sortPageByKey();
        return 0;
    }

    @Override
    protected int delete(int key) {
        for (int i = 0; i < recordAmount; i++) {
            if (data[i].key == key) {

                if (data[i].deleted) return -1;

                data[i].deleted = true;
                return 0;
            }
        }
        return -1;
    }

    @Override
    protected int updateHard(int key, TRecord record) {
        int pos = getPosFromRecord(record);
        if (pos == -1) return -1;
        data[pos] = record;
        return pos;
    }

    @Override
    protected int updateSoft(int key, TRecord record) {
        int pos = getPosFromRecord(record);
        if (pos == -1) return -1;
        data[pos].a = record.a;
        data[pos].b = record.b;
        data[pos].h = record.h;
        return 0;
    }

    @Override
    protected TRecord getRecord(int key) {
        if (isEmpty()) {
            throw new IllegalStateException("Trying to get TRecord from empty page");
        }
        for (int i = 0; i < pageSize; i++) {
            if (data[i].key == key && !data[i].deleted) {
                return data[i];
            }
        }
        return null;
    }

    @Override
    protected int getSizeBytesRest() {
        return new TRecord().getSizeBytes() * pageSize;

    }

    public boolean isOverflown() {
        for (int i = 0; i < recordAmount; i++) {
            if (data[i].next.exists()) return true;
        }
        return false;
    }

    public TRecord findPrevious(int key) {
        int lastPos = -1;
        for (int i = 0; i < recordAmount; i++) {
            if (data[i].key < key) {
                lastPos = i;
            }
        }
        return lastPos == -1 ? null : data[lastPos];
    }

    public void sortPageByKey() {
        Arrays.sort(
            data,
            0,
            recordAmount,
            Comparator.comparingInt(r -> r.key)
        );
    }
}
