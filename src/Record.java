import java.io.*;

public abstract class Record implements IDataSerializable {

    int key;
    boolean deleted;
    @Override
    public abstract String toString();
}
