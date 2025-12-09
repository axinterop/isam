import java.io.*;
import java.nio.file.*;
import java.util.*;

public class Main {


    static boolean CONTROL = false;
    static int MAX_TO_PRINT = 50;
    static boolean LIMIT_PRINT = false;

    public static void main(String[] args) throws Exception {
        cleanup();
        MAX_TO_PRINT = 30;
        LIMIT_PRINT = true;
        CONTROL = false;

        BTree bt = new BTree(2);

        int key = 100;
        System.out.println(bt.getDiskStats());

        for (int i = 0; i < 5; i++) {
            bt.insertRecord(key + i, new Record(i, 2.0 + i, 3.0  + i, 4.0  + i));
        }
        bt.displayTreeStructure();
        Record r = bt.getRecord(100);

        System.out.println(bt.getDiskStats());
        System.out.println("Record " + r);
    }

    public enum File {
        RECORDS,
        BTREE
    }


    static List<Record> readAllRecords(String filename) throws IOException {
        List<Record> res = new ArrayList<>();
        try (
            DataInputStream in = new DataInputStream(
                new BufferedInputStream(new FileInputStream(filename))
            )
        ) {
            while (true) {
                try {
                    res.add(Record.readFrom(in));
                } catch (EOFException e) {
                    break;
                }
            }
        }
        return res;
    }

    static void cleanup() {
        try {
            Files.deleteIfExists(Paths.get(DiskDrive.getFileFromFileType(File.BTREE)));
            Files.deleteIfExists(Paths.get(DiskDrive.getFileFromFileType(File.RECORDS)));
        } catch (IOException e) {
        }
    }


}
