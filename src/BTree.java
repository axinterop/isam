import java.io.*;
import java.util.Arrays;


public class BTree {
    public final int order;
    private DiskDrive disk;
    private long lastRecordAddress = 0;
    private int nextPageNumber = 0;
    private int rootPageNumber = -1;

    public class KeyPair {
        public int key;
        public int record_address;

        public KeyPair() {
            this.key = -1;
            this.record_address = -1;
        }

        public KeyPair(int key, int record_address) {
            this.key = key;
            this.record_address = record_address;
        }

        public boolean isEmpty() {
            return key == -1;
        }
    }


    public class BTreePage {
        // Metadata
        private final int order;

        // Data stored in file
        public int address;
        public KeyPair[] keyPairs;
        public int[] childrenPages;
        public int parentAddress;
        private int keyCount = 0;

        BTreePage(int order_in, int parent_page) {
            this.order = order_in;
            // Maximum 2*d keys
            keyPairs = new KeyPair[2 * order_in];
            for (int i = 0; i < keyPairs.length; i++) {
                keyPairs[i] = new KeyPair();
            }

            // Maximum 2*d + 1 children
            childrenPages = new int[2 * order_in + 1];
            // All children start as leaf pointers
            Arrays.fill(childrenPages, -1);

            parentAddress = parent_page;
            this.address = -1;  // Will be set when written to disk
        }

        public byte[] serialize() {
            int pageSize = 4 + (2 * order) * 8 + (2 * order + 1) * 4;
            ByteArrayOutputStream baos = new ByteArrayOutputStream(pageSize);
            DataOutputStream dos = new DataOutputStream(baos);

            try {
                // Write parent address
                dos.writeInt(parentAddress);

                // Write all key pairs (2*order maximum)
                for (int i = 0; i < 2 * order; i++) {
                    dos.writeInt(keyPairs[i].key);
                    dos.writeInt(keyPairs[i].record_address);
                }

                // Write all child page numbers (2*order + 1 maximum)
                for (int i = 0; i < 2 * order + 1; i++) {
                    dos.writeInt(childrenPages[i]);
                }

                dos.flush();
                return baos.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize BTreePage", e);
            }
        }

        public void deserialize(byte[] data) {
            ByteArrayInputStream bais = new ByteArrayInputStream(data);
            DataInputStream dis = new DataInputStream(bais);

            try {
                // Read parent address
                parentAddress = dis.readInt();

                // Read all key pairs
                for (int i = 0; i < 2 * order; i++) {
                    int key = dis.readInt();
                    int addr = dis.readInt();
                    keyPairs[i] = new KeyPair(key, addr);
                }

                // Read all child page numbers
                for (int i = 0; i < 2 * order + 1; i++) {
                    childrenPages[i] = dis.readInt();
                }

                // Count actual keys (non-empty keypairs)
                keyCount = 0;
                for (KeyPair kp : keyPairs) {
                    if (!kp.isEmpty()) {
                        keyCount++;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to deserialize BTreePage", e);
            }
        }

        /**
         * Get current count of keys in this page
         */
        public int getKeyCount() {
            return keyCount;
        }

        public boolean isFull() {
            return keyCount >= 2 * order;
        }

        public boolean isLeaf() {
            for (int child : childrenPages) {
                if (child != -1) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Find position where key should be inserted (in sorted order)
         * Returns the index where key should go
         */
        public int findInsertPosition(int key) {
            int pos = 0;
            while (pos < keyCount && keyPairs[pos].key < key) {
                pos++;
            }
            return pos;
        }

        /**
         * Insert a key-address pair into this page in sorted order
         * Assumes page is not full
         */
        public void insertKeyPair(int key, int record_address) {
            if (isFull()) {
                throw new IllegalStateException("Page is full, cannot insert");
            }

            int pos = findInsertPosition(key);

            // Shift existing keys to the right
            for (int i = keyCount; i > pos; i--) {
                keyPairs[i] = new KeyPair(keyPairs[i - 1].key, keyPairs[i - 1].record_address);
            }

            // Insert new key pair
            keyPairs[pos] = new KeyPair(key, record_address);
            keyCount++;
        }

        /**
         * Get the median key index (for splitting purposes)
         * For order=2: max 4 keys, median is at index 1 (or 2)
         */
        public int getMedianIndex() {
            return order - 1;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("BTreePage[addr=").append(address)
                .append(" parent=").append(parentAddress)
                .append(" keyCount=").append(keyCount)
                .append(" keys=(");
            for (int i = 0; i < keyCount; i++) {
                if (i > 0) sb.append(", ");
                sb.append(keyPairs[i].key).append(":").append(keyPairs[i].record_address);
            }
            sb.append(")]");
            return sb.toString();
        }
    }

    public BTree(int order) {
        this.order = order;
        int page_size = 2 * order;  // For RECORDS_FILE paging
        disk = new DiskDrive(page_size);
    }

    /**
     * Insert a record with given key
     * Steps:
     * 1. Assign record address (increment lastRecordAddress)
     * 2. Write record to RECORDS_FILE
     * 3. Create/get root page
     * 4. Insert key-pair into root (for now, no splitting)
     * 5. Write root page to BTREE_FILE
     */
    public void insertRecord(int key, Record record) throws IOException {
        // Step 1: Assign record address
        int recordAddress = (int) lastRecordAddress;
        record.id = recordAddress;  // Set record ID to its address
        lastRecordAddress++;

        // Step 2: Write record to RECORDS_FILE
        disk.writeRecord(Main.File.RECORDS, record);

        // Step 3: Get or create root page
        BTreePage rootPage = getRootPage();

        // Step 4: Insert into root (basic: no splitting for now)
        if (!rootPage.isFull()) {
            rootPage.insertKeyPair(key, recordAddress);
        } else {
            throw new IllegalStateException("Root page is full - splitting not yet implemented");
        }

        // Step 5: Write root page back to disk
        writeBTreePage(rootPage);

    }

    /**
     * Get or create the root page (page 0)
     * If root doesn't exist, create it
     */
    private BTreePage getRootPage() throws IOException {
        if (rootPageNumber == -1) {
            // Create root page
            BTreePage root = new BTreePage(order, -2);  // -2 indicates no parent (root)
            root.address = 0;
            rootPageNumber = 0;
            nextPageNumber = 1;
            writeBTreePage(root);
            return root;
        } else {
            // Read existing root page
            return readBTreePage(rootPageNumber);
        }
    }

    /**
     * Write a BTreePage to BTREE_FILE at its address
     */
    private void writeBTreePage(BTreePage page) throws IOException {
        byte[] serialized = page.serialize();
        disk.writeBTreePage(Main.File.BTREE, page.address, serialized);
    }

    /**
     * Read a BTreePage from BTREE_FILE at given page number
     */
    private BTreePage readBTreePage(int pageNumber) throws IOException {
        byte[] serialized = disk.readBTreePage(Main.File.BTREE, pageNumber);
        if (serialized == null) {
            throw new IOException("Failed to read BTree page " + pageNumber);
        }
        BTreePage page = new BTreePage(order, -1);
        page.address = pageNumber;
        page.deserialize(serialized);
        return page;
    }

    /**
     * Allocate a new page number (for splitting)
     */
    public int allocateNewPage() {
        return nextPageNumber++;
    }

    public Record getRecord(int key) throws IOException {
        // 1. Look for key in BTREE
        // 2. If found: get record_address from keyPair
        // 3. Read record from RECORDS_FILE using address

        BTreePage rootPage = getRootPage();
        int recordAddress = searchInPage(rootPage, key);

        if (recordAddress >= 0) {
            return disk.readRecord(Main.File.RECORDS, recordAddress);
        }
        return null;
    }

    /**
     * Search for a key within a single page (linear search for now)
     * Returns record address if found, -1 otherwise
     */
    private int searchInPage(BTreePage page, int key) {
        for (int i = 0; i < page.getKeyCount(); i++) {
            if (page.keyPairs[i].key == key) {
                return page.keyPairs[i].record_address;
            }
        }
        return -1;
    }

    public void removeRecord(int key) throws IOException {
        // Very complicated - not implemented yet
        throw new UnsupportedOperationException("Remove not yet implemented");
    }

    public DiskDrive.DiskStats getDiskStats() {
        return disk.getStats();
    }

    public BTreePage getRootPageDirect() throws IOException {
        return getRootPage();
    }
}


