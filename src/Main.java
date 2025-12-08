import java.io.*;
import java.nio.file.*;
import java.util.*;

public class Main {


    static int PAGE_SIZE = 4;
    static final int RECORD_SIZE = 24;
    static boolean CONTROL = false;
    static int MAX_TO_PRINT = 50;
    static boolean LIMIT_PRINT = false;

    public static void main(String[] args) throws Exception {
        cleanup();
        MAX_TO_PRINT = 30;
        LIMIT_PRINT = true;
        CONTROL = false;

        BTree bt = new BTree(2);
        bt.insertRecord(0, new Record(0, 1.0, 2.0, 3.0));
        bt.insertRecord(2, new Record(1, 2.0, 3.0, 4.0));

        Record r = bt.getRecord(1);
        System.out.println("Record " + r);
    }

    public enum File {
        RECORDS,
        BTREE
    }


    // --- utils ----
//    static void generateInputFile(String filename, int n, Random rnd) throws IOException {
//        try (
//            DataOutputStream out = new DataOutputStream(
//                new BufferedOutputStream(new FileOutputStream(filename))
//            )
//        ) {
//            for (int i = 0; i < n; i++) {
//                double a = 1.0 + rnd.nextDouble() * 100.0;
//                double b = 1.0 + rnd.nextDouble() * 100.0;
//                double h = 0.1 + rnd.nextDouble() * 50.0;
//                Record r = new Record(a, b, h);
//                r.writeTo(out);
//            }
//        }
//    }

//    static int readInputFromFile(String file_txt, String file_input)
//        throws IOException {
//        try (
//            BufferedReader in = Files.newBufferedReader(Paths.get(file_txt));
//            DataOutputStream out = new DataOutputStream(
//                new BufferedOutputStream(new FileOutputStream(file_input))
//            )
//        ) {
//            int n = 0;
//            String line;
//            while ((line = in.readLine()) != null) {
//                line = line.trim();
//                if (line.isEmpty()) {
//                    continue;
//                }
//                double current_area = Double.parseDouble(line.trim());
//
//                double a = current_area;
//                double b = 0.0;
//                double h = 2.0;
//                Record r = new Record(0, a, b, h);
//                r.writeTo(out);
//                n++;
//            }
//            return n;
//        }
//    }

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
