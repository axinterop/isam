import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class Record implements Comparable<Record>, Serializable {

    int id;
    double a, b, h;

    public Record() {
    }

    public Record(int rec_id, double a, double b, double h) {
        this.id = rec_id;
        this.a = a;
        this.b = b;
        this.h = h;
    }

    public static Record readFrom(DataInput in) throws IOException {
        Record r = new Record();
        r.a = in.readDouble();
        r.b = in.readDouble();
        r.h = in.readDouble();
        return r;
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeDouble(a);
        out.writeDouble(b);
        out.writeDouble(h);
    }

    @Override
    public int compareTo(Record o) {
        return Double.compare(id, o.id);
    }

    @Override
    public String toString() {
        return String.format(
            "ID:%d a=%.3f b=%.3f h=%.3f",
            id,
            a,
            b,
            h
        );
    }

    static public int sizeBytes() {
        return 32;
    }
}
