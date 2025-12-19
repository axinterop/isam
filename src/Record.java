import java.io.*;

public abstract class Record implements IDataSerializable {

    int key;
    @Override
    public abstract String toString();
}
