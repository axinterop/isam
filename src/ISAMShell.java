import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.Scanner;

public class ISAMShell {

    private final ISAM isam;

    private final Random random = new Random(12345);

    ArrayList<String> executedCommands;

    public ISAMShell(ISAM isam) {
        this.isam = isam;
        this.executedCommands = new ArrayList<>();
    }

    private void printCommands() {
        System.out.println("Commands:");
        System.out.println("  [g]et <key>");
        System.out.println("  [i|insert] <key> <a?> <b?> <h?>");
        System.out.println("  [u]pdate <key> <a> <b> <h>");
        System.out.println("  [d]elete <key>");
        System.out.println("  [r|random] [<max>|<min> <max> <amount?>]  - random insert");
        System.out.println("  [dr|delete random] [<amount>|<min> <max> <amount>]");
        System.out.println();
        System.out.println("  [p]rint");
        System.out.println("  [ps|print sequence] <[all]?>              - 'all' parameters shows deleted records");
        System.out.println("  [s]tats");
        System.out.println("  auto                                      - toggle auto reorganization (default is 'off')");
        System.out.println("  reorganize <[f]?>                         - use 'f' to force reorganization");
        System.out.println();
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
                if (!isam.autoReorganization) {
                    System.out.println("Automatic reorganization is disabled. Database won't be reorganized during operations.");
                }
                if (isam.overflowReachedThreshold()) {
                    String s = String.format("Overflow file reached threshold (%.2f/%.2f).", isam.currentOverflowRatio() * 100, isam.overflowThreshold * 100);
                    System.out.println(s);
                }
                if (isam.deletionReachedThreshold()) {
                    String s = String.format("Deletion counter reached threshold (%.2f/%.2f).", isam.currentDeletionRatio() * 100, isam.deletionThreshold * 100);
                    System.out.println(s);
                }
                if (isam.needsReorganization()) {
                    System.out.println("Database needs reorganization.");
                    if (!isam.autoReorganization) {
                        System.out.println("You can run it manually with `reorganize` command or enable with 'auto' command.");
                    } else {
                        System.out.println("It will be triggered automatically on any upcoming non-read command.");
                    }
                }


                System.out.print("> ");
                if (!scanner.hasNextLine()) {
                    break;
                }

                String line = scanner.nextLine().trim();
                if (line.isEmpty()) continue;

                if (!processCommand(line)) break;  // Exits on 'exit' command
            }
        }
    }

    public boolean processCommand(String line) {
        String[] parts = line.split("\\s+");
        String cmd = parts[0].toLowerCase();

        try {
            switch (cmd) {
                case "get":
                case "g": {
                    if (parts.length != 2) {
                        System.out.println("Usage: [g|get] <key>");
                        break;
                    }
                    int key = Integer.parseInt(parts[1]);
                    TRecord rec = isam.get(key);
                    if (rec == null) {
                        System.out.println("Not found.");
                    } else {
                        System.out.println("Record: " + rec);
                    }
                    executedCommands.add(line);
                    break;
                }
                case "insert":
                case "i": {
                    if (parts.length != 2 && parts.length != 5) {
                        System.out.println("Usage: [i|insert] <key> <a?> <b?> <h?>");
                        break;
                    }

                    double a = 1;
                    double b = 2;
                    double h = 3;
                    if (parts.length == 5) {
                        a = Double.parseDouble(parts[2]);
                        b = Double.parseDouble(parts[3]);
                        h = Double.parseDouble(parts[4]);
                    }

                    int key = Integer.parseInt(parts[1]);
                    TRecord rec = new TRecord(key, a, b, h);
                    int res = isam.insert(rec);
                    System.out.println("Inserted key=" + key + ", result=" + res);
                    executedCommands.add(line);
                    break;
                }
                case "update":
                case "u": {
                    if (parts.length != 5) {
                        System.out.println("Usage: [u|update] <key> <a> <b> <h>");
                        break;
                    }
                    int key = Integer.parseInt(parts[1]);
                    int a = Integer.parseInt(parts[2]);
                    int b = Integer.parseInt(parts[3]);
                    int h = Integer.parseInt(parts[4]);
                    TRecord rec = new TRecord(key, a, b, h);
                    int result = isam.update(rec);
                    if (result == -1) {
                        System.out.println("Not found.");
                    } else {
                        System.out.println("Record updated.");
                    }
                    executedCommands.add(line);
                    break;
                }
                case "delete":
                case "d": {
                    if (parts.length != 2) {
                        System.out.println("Usage: [d|delete] <key>");
                        break;
                    }
                    int key = Integer.parseInt(parts[1]);
                    int result = isam.delete(key);
                    if (result == -1) {
                        System.out.println("Not found.");
                    } else {
                        System.out.println("Record deleted.");
                    }
                    executedCommands.add(line);
                    break;
                }
                case "random":
                case "r": {
                    if (parts.length != 2 && parts.length != 3 && parts.length != 4) {
                        System.out.println("Usage:\n\t[r|random] <max>");
                        System.out.println("\t[r|random] <min> <max> <amount?>");
                        break;
                    }

                    int minRandom = Integer.parseInt(parts[1]);
                    int maxRandom;
                    if (parts.length == 2) {
                        maxRandom = minRandom;
                        minRandom = 0;
                    } else {
                        maxRandom = Integer.parseInt(parts[2]);
                    }
                    int amount = maxRandom - minRandom;
                    if (parts.length == 4) {
                        amount = Integer.parseInt(parts[3]);
                    }
                    int[] insertedKeys = new int[amount];
                    Arrays.fill(insertedKeys, -1);
                    RandomPool rp = new RandomPool(random, minRandom, maxRandom);

                    int insertedAmount = isam.randomInsert(amount, rp, insertedKeys);

                    if (insertedAmount == 0) {
                        System.out.println("Specified range of random numbers is exhausted. Try different `min`/`max` value.");
                        executedCommands.add(line);
                        break;
                    }

                    System.out.println("Inserted " + insertedAmount + " random records: ");
                    for (int k : insertedKeys) {
                        if (k == -1) continue;
                        System.out.print(k + " ");
                    }
                    System.out.println();

                    executedCommands.add(line);
                    break;
                }

                case "delete random":
                case "dr": {
                    if (parts.length != 2 && parts.length != 4) {
                        System.out.println("Usage:\n\t[dr|delete random] <amount>");
                        System.out.println("\t[dr|delete random] <min> <max> <amount>");
                        break;
                    }

                    int amount, minRandom, maxRandom;

                    if (parts.length == 2) {
                        amount = Integer.parseInt(parts[1]);
                        minRandom = 0;
                        maxRandom = isam.insertedRecordAmount();
                    } else {
                        minRandom = Integer.parseInt(parts[1]);
                        maxRandom = Integer.parseInt(parts[2]);
                        amount = Integer.parseInt(parts[3]);
                    }

                    int[] deletedKeys = new int[amount];
                    Arrays.fill(deletedKeys, -1);

                    RandomPool rp = new RandomPool(random, minRandom, maxRandom);

                    int deletedAmount = isam.randomDelete(amount, rp, deletedKeys);

                    if (deletedAmount == 0) {
                        System.out.println("Specified range of random numbers is exhausted. Try different `min`/`max` value.");
                        executedCommands.add(line);
                        break;
                    }
                    System.out.println("Randomly deleted " + deletedAmount + " records: ");
                    for (int k : deletedKeys) {
                        if (k == -1) continue;
                        System.out.print(k + " ");
                    }
                    System.out.println();
                    executedCommands.add(line);
                    break;
                }
                case "print":
                case "p": {
                    isam.print();
                    break;
                }
                case "print sequence":
                case "ps": {
                    if (parts.length != 1 && parts.length != 2) {
                        System.out.println("Usage: [ps|print sequence] <all?>");
                        break;
                    }

                    System.out.println("Sequence: ");

                    boolean showDeleted = false;
                    if (parts.length == 2) {
                        if (parts[1].equals("all")) {
                            showDeleted = true;
                            System.out.println("(deleted keys are followed with underscore '_')");
                        }
                    }
                    isam.printInSequenceRW(showDeleted);
                    break;
                }

                case "stats":
                case "s": {
                    isam.printStats();
                    break;
                }
                case "auto": {
                    isam.autoReorganization = !isam.autoReorganization;
                    if (isam.autoReorganization) {
                        System.out.println("Auto reorganization enabled.");
                    } else {
                        System.out.println("Auto reorganization disabled.");
                    }
                    break;
                }
                case "reorganize": {
                    boolean force = false;
                    if (parts.length == 2 && parts[1].equals("f")) {
                        force = true;
                    }
                    if (isam.reorganize(force) == 0) {
                        System.out.println("Reorganized.");
                    } else {
                        if (!force) {
                            System.out.println("No need for reorganization. If you want to force it, use 'reorganize f'.");
                        } else {
                            System.out.println("Not reorganized!");
                        }
                    }
                    executedCommands.add(line);
                    break;
                }

                case "flush":
                case "f": {
                    isam.flush();
                    System.out.println("Flushed.");
                    break;
                }
                case "cleanup": {
                    isam.cleanupFull();
                    executedCommands.clear();
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
                    return false;
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
        return true;
    }
}
