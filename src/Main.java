import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.*;

public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: java Main <pageSize> <overflowThreshold> <deleteThreshold> [input_file]");
            System.exit(1);
        }

        int pageSize = Integer.parseInt(args[0]);
        double overflowThreshold = Double.parseDouble(args[1]);
        double deletionThreshold = Double.parseDouble(args[2]);
        String inputFile = args.length > 3 ? args[3] : null;

        String INDEX_FILE = "index.dat";
        String RECORDS_FILE = "records.dat";
        String OVERFLOW_FILE = "overflow.dat";

        ISAM isam = new ISAM(INDEX_FILE, RECORDS_FILE, OVERFLOW_FILE, pageSize);
        isam.setOverflowThreshold(overflowThreshold);
        isam.setDeletionThreshold(deletionThreshold);

        ISAMShell shell = new ISAMShell(isam);

        boolean isInteractive = System.console() != null;

        if (isInteractive) {
            System.out.println("No file detected. Entering interactive mode");
            shell.run();
        } else {
            processFile(shell, inputFile);
            System.out.println("\nInput file processed. Entering interactive mode");
            shell.run();
        }
    }

    private static void processFile(ISAMShell shell, String filename) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;
                shell.processCommand(line);
            }
        } catch (Exception e) {
            System.err.println("Error processing file: " + e.getMessage());
        }
    }

}
