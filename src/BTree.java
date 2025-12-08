import java.io.IOException;


public class BTree {
    public final int order;
    private DiskDrive disk;

    public class KeyPair {
        public int key;
        public int record_address;
    }


    public class BTreePage {
        // Metadata
        private final int order;

        // Data stored in file
        public int address;
        public KeyPair[] keyPairs;
        public BTreePage[] childrenPages;
        public int parentAddress;

        BTreePage(int order_in, int parent_page) {
            this.order = order_in;
            keyPairs = new KeyPair[order];
            childrenPages = new BTreePage[order + 1];
            parentAddress = parent_page;
        }
    }

    public BTree(int order) {
        this.order = order;
        int page_size = 2 * order;  // each node at max 2 * order
        disk = new DiskDrive(page_size);
    }

    public void insertRecord(int key, Record record) throws IOException {
        // 1. Look, if key doesn't already exist in a tree (BTREE)
        // 2. If not: get the page it should be on (BTREE)
        // 3. Look for available record_address (RECORDS)
        // 4. When found, insert record there (RECORDS)
        // 5. Get the address back
        // 6. Create keyPair
        // 7. Insert keyPair (BTREE)
        // 8. Sort keyPairs with pageAddresses (BTREE)
        disk.writeRecord(Main.File.RECORDS, record);
    }

    public Record getRecord(int key) throws IOException {
        // 1. Look, for key (BTREE)
        // 2. If it exists: get record_address from keyPair
        // 3. Calculate record_page, record_index from record_address
        // 4. Get record_page page (RECORDS)
        // 5. Get record from record_index
        // 6. Return record
        int recordIndex = key; // handle it
        return disk.readRecord(Main.File.RECORDS, recordIndex);
    }

    public void removeRecord(int key) throws IOException {
        // Very complicated
    }

    public DiskDrive.DiskStats getDiskStats() {
        return disk.getStats();
    }
}


