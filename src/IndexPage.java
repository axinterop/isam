import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IndexPage extends Page<IndexRecord> {

    @Override
    protected IndexRecord createRecordInstance() {
        return new IndexRecord();
    }

    @Override
    protected IndexRecord[] createRecordArray() {
        return new IndexRecord[pageSize];
    }

    public IndexPage(int pageSize) {
        super(pageSize);
    }

    @Override
    protected void serializeBody(DataOutputStream dos) throws IOException {
        for (IndexRecord indexRecord : data) {
            indexRecord.serializeData(dos);
        }
    }

    @Override
    protected void deserializeBody(DataInputStream dis) throws IOException {
        for (int i = 0; i < data.length; i++) {
            IndexRecord ir = new IndexRecord();
            ir.deserializeData(dis);
            data[i] = ir;
        }
    }

    @Override
    protected int insertRecord(IndexRecord record) {

    }

    @Override
    protected void deleteRecord(int key) {

    }

    @Override
    protected void updateRecord(int key) {

    }

    @Override
    protected void getRecord(int key) {

    }

    @Override
    protected int getSizeBytesRest() {
        return new IndexRecord().getSizeBytes() * pageSize;
    }


}
