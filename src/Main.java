import java.util.*;

public class Main {

    public static void main(String[] args) throws Exception {
        String INDEX_FILE = "index.dat";
        String RECORDS_FILE = "records.dat";
        String OVERFLOW_FILE = "overflow.dat";

        int pageSize = 4;

        // TODO: Update func

        ISAM isam = new ISAM(INDEX_FILE, RECORDS_FILE, OVERFLOW_FILE, pageSize);
        ISAMShell shell = new ISAMShell(isam);
        shell.run();
    }
}
