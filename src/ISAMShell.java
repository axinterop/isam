import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;

public class ISAMShell {

    private final ISAM isam;

    private final Random random = new Random(12345);

    public ISAMShell(ISAM isam) {
        this.isam = isam;
    }

    private void printCommands() {
        System.out.println("Commands:");
        System.out.println("  [i]nsert <key> <a> <b> <h>");
        System.out.println("  [r]andom <amount> <maxRandom>");
        System.out.println("  [g]et <key>");
        System.out.println("  [p]rint");
        System.out.println("  [ps|print sequence]");
        System.out.println("  [s]tats");
        System.out.println("  cleanup");
        System.out.println("  [f]lush");
        System.out.println("  [h]elp - show commands");
        System.out.println("  [e]xit");
    }

    public void run() {
        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("ISAM interactive shell");
            printCommands();

            while (true) {
                System.out.print("> ");
                if (!scanner.hasNextLine()) {
                    break;
                }
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) {
                    continue;
                }

                String[] parts = line.split("\\s+");
                String cmd = parts[0].toLowerCase();

                try {
                    switch (cmd) {
                        case "insert":
                        case "i": {
                            if (parts.length != 2 && parts.length != 5) {
                                System.out.println("Usage: [i|insert] <key> <a?> <b?> <h?>");
                                break;
                            }

                            int key = Integer.parseInt(parts[1]);
                            double a = 1;
                            double b = 2;
                            double h = 3;
                            if (parts.length == 5) {
                                a = Double.parseDouble(parts[2]);
                                b = Double.parseDouble(parts[3]);
                                h = Double.parseDouble(parts[4]);
                            }
                            TRecord rec = new TRecord(key, a, b, h);
                            int res = isam.insert(rec);
                            System.out.println("Inserted, result=" + res);
                            break;
                        }
                        case "random":
                        case "r": {
                            if (parts.length != 3 && parts.length != 4) {
                                System.out.println("Usage: [r|random] <min> <max> <amount?>");
                                break;
                            }
                            int minRandom = Integer.parseInt(parts[1]);
                            int maxRandom = Integer.parseInt(parts[2]);

                            int amount = maxRandom -  minRandom;
                            if (parts.length == 4) {
                                amount = Integer.parseInt(parts[3]);
                            }
                            int[] insertedKeys = new int[amount];
                            Arrays.fill(insertedKeys, -1);
                            int i = 0;

                            RandomPool rp = new RandomPool(random, minRandom, maxRandom);

                            while (i < amount && !rp.isEmpty()) {
                                int value = rp.next();
                                try {
                                    isam.insert(new TRecord(value, value, value, value));
                                    insertedKeys[i++] = value;
                                } catch (Exception ignored) {
                                }
                            }

                            if (i == 0) {
                                System.out.println("Specified range of random numbers is exhausted. Try different `min`/`max` value.");
                                break;
                            }
                            System.out.println("Inserted " + i + " random records: ");
                            for (int k : insertedKeys) {
                                if (k == -1) continue;
                                System.out.print(k + " ");
                            }
                            System.out.println();
                            break;
                        }
                        case "get":
                        case "g": {
                            if (parts.length != 2) {
                                System.out.println("Usage: [g|get] <key>");
                                break;
                            }
                            int key = Integer.parseInt(parts[1]);
                            TRecord rec = isam.get(key);
                            if (rec == null) {
                                System.out.println("Not found");
                            } else {
                                System.out.println("Record: " + rec);
                            }
                            break;
                        }
                        case "print":
                        case "p": {
                            isam.print();
                            break;
                        }
                        case "print sequence":
                        case "ps": {
                            System.out.println("Sequence: ");
                            isam.printInSequenceRW();
                            break;
                        }

                        case "stats":
                        case "s": {
                            ISAM.IOStats io = isam.getStats();
                            System.out.println("Stats: ");
                            System.out.println("- Index reads: " + io.indexReads);
                            System.out.println("- Index writes: " + io.indexWrites);
                            System.out.println("- Records reads: " + io.recordsReads);
                            System.out.println("- Records writes: " + io.recordsWrites);
                            System.out.println("- Overflow reads: " + io.overflowReads);
                            System.out.println("- Overflow writes: " + io.overflowWrites);
                            System.out.println("== Total reads: " + io.totalReads());
                            System.out.println("== Total writes: " + io.totalWrites());
                            break;
                        }
                        case "flush":
                        case "f": {
                            isam.flush();
                            System.out.println("Flushed.");
                            break;
                        }
                        case "cleanup": {
                            isam.cleanup();
                            System.out.println("Cleaned up.");
                            break;
                        }
                        case "help":
                        case "h": {
                            printCommands();
                            break;
                        }

                        case "exit":
                        case "e": {
                            isam.flush();
                            System.out.println("Bye.");
                            return;
                        }
                        default:
                            System.out.println("Unknown command: " + cmd);
                    }
                } catch (IllegalStateException | IllegalArgumentException e) {
                    System.out.println("Error: " + e.getMessage());
                } catch (IOException e) {
                    System.out.println("IO error: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }
}
