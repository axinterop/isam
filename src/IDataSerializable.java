import java.io.*;

public interface IDataSerializable {
    void serializeData(DataOutputStream dos) throws IOException;
    void deserializeData(DataInputStream dis) throws IOException;

    default byte[] serialize() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(getSizeBytes());
        DataOutputStream dos = new DataOutputStream(baos);

        try {
            this.serializeData(dos);
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize " + this,e);
        }
    }

    default void deserialize(byte[] data) {
        if (data.length != getSizeBytes()) {
            throw new RuntimeException("Invalid byte size of " + this);
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        DataInputStream dis = new DataInputStream(bais);

        try {
            this.deserializeData(dis);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize " + this, e);
        }
    }

    int getSizeBytes();
}
