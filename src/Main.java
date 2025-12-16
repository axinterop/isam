import java.util.*;

public class Main {


    static boolean CONTROL = false;
    static int MAX_TO_PRINT = 50;
    static boolean LIMIT_PRINT = false;

    public static void main(String[] args) throws Exception {
//        cleanup();
        MAX_TO_PRINT = 30;
        LIMIT_PRINT = true;
        CONTROL = false;

        String INDEX_FILE = "index.dat";
        String RECORDS_FILE = "records.dat";
        String OVERFLOW_FILE = "overflow.dat";

        ISAM isam = new ISAM(INDEX_FILE, RECORDS_FILE, OVERFLOW_FILE, 4);

        Random random = new Random(12345);
        int randomNums = 50;
        while(randomNums > 0) {
            int value = random.nextInt() % 100;
            value = Math.abs(value);
            try {
                isam.insert(new TRecord(value, 1, 2, 3));
                isam.print();
                randomNums--;
            } catch (Exception ignored) {}
        }
        isam.insert(new TRecord(101, 5, 5, 5));
        isam.print();
        TRecord r = isam.getRecord(86);
        r.deleted = true;
        r = isam.getRecord(101);
        isam.print();

    }
}
