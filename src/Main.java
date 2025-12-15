import java.io.*;
import java.nio.file.*;
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

        TRecord test_record = new TRecord(0, 1, 2, 3);

        int result = isam.insertRecord(new TRecord(0, 1, 2, 3));
        TRecord recordAfterGet = isam.getRecord(0);
        assert test_record.equals(recordAfterGet);

        isam.flush();


    }


//    static List<Record> readAllRecords(String filename) throws IOException {
//        List<Record> res = new ArrayList<>();
//        try (
//            DataInputStream in = new DataInputStream(
//                new BufferedInputStream(new FileInputStream(filename))
//            )
//        ) {
//            while (true) {
//                try {
//                    res.add(Record.readFrom(in));
//                } catch (EOFException e) {
//                    break;
//                }
//            }
//        }
//        return res;
//    }
//
//    static void cleanup() {
//        try {
//            Files.deleteIfExists(Paths.get(DiskDrive.getFile(DiskDrive.File.INDEX)));
//            Files.deleteIfExists(Paths.get(DiskDrive.getFile(DiskDrive.File.RECORDS)));
//            Files.deleteIfExists(Paths.get(DiskDrive.getFile(DiskDrive.File.OVERFLOW)));
//        } catch (IOException e) {
//        }
//    }


}
