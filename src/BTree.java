import java.io.*;
import java.util.Arrays;


public class BTree {
    public final int order;
    private DiskDrive disk;
    private int lastRecordAddress = 0;
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

    private class SearchResult {
        int recordAddress;  // -1 if not found
        int childPagePointer;  // which child to follow if not found

        SearchResult(int recordAddress, int childPagePointer) {
            this.recordAddress = recordAddress;
            this.childPagePointer = childPagePointer;
        }
    }

    /**
     * Result of traversing tree to find leaf
     */
    private class TraversalResult {
        BTreePage leafPage;
        int parentPageNumber;
        int positionInParent;  // index in parent's childrenPages[]

        TraversalResult(BTreePage leafPage, int parentPageNumber, int positionInParent) {
            this.leafPage = leafPage;
            this.parentPageNumber = parentPageNumber;
            this.positionInParent = positionInParent;
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
         * Binary search to find insertion position or detect existing key
         * Returns: index where key should go (or where it was found)
         * Throws: RuntimeException if key already exists
         */
        public int findInsertPosition(int key) {
            int left = 0;
            int right = keyCount - 1;

            while (left <= right) {
                int mid = left + (right - left) / 2;
                if (keyPairs[mid].key == key) {
                    throw new RuntimeException("Key already exists: " + key);
                } else if (keyPairs[mid].key < key) {
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }

            return left;  // Position where key should be inserted
        }

        /**
         * Binary search to find child pointer for key navigation
         * Returns: index in childrenPages[] to follow
         * If key exists, throws exception
         */
        public int findChildPointer(int key) {
            int left = 0;
            int right = keyCount - 1;

            while (left <= right) {
                int mid = left + (right - left) / 2;
                if (keyPairs[mid].key == key) {
                    throw new RuntimeException("Key already exists: " + key);
                } else if (keyPairs[mid].key < key) {
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }

            // left is now the index of first key >= key
            return left;  // childrenPages[left] is the child to follow
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
            return (2 * order) / 2;
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
     * Public API - handles record assignment and tree insertion
     */
    public void insertRecord(int key, Record record) throws IOException {
        int saved_id = record.id;
        int recordAddress = lastRecordAddress;
        record.id = recordAddress;

        int result = findLeafAndInsert(key, recordAddress);

        if (result == -2) {
            throw new RuntimeException("Key already exists: " + key);
        }

        if (result == -1) {
            System.out.println("SPLITTING (not implemented)");
        }

        // SPLIT

        if (result >= 0) {
            System.out.println("Insert successful");
            lastRecordAddress++;
            disk.writeRecord(Main.File.RECORDS, record);
        } else {
            record.id = saved_id;
        }

    }

    /**
     * Find the correct leaf page and attempt insertion with compensation
     * Returns: -2 if duplicate, -1 if splitting needed, >= 0 if success
     */
    private int findLeafAndInsert(int key, int recordAddress) throws IOException {
        // Step 1: Root doesn't exist yet - create it and insert
        if (rootPageNumber == -1) {
            BTreePage root = getRootPage();
            try {
                root.insertKeyPair(key, recordAddress);
                writeBTreePage(root);
                return 0;  // Success
            } catch (RuntimeException e) {
                if (e.getMessage().contains("already exists")) {
                    return -2;  // Duplicate
                }
                throw e;
            }
        }

        // Step 2: Traverse tree to find correct leaf
        TraversalResult traversal = findLeaf(key);
        if (traversal == null) {
            return -2;  // Duplicate detected during traversal
        }

        // Step 3: Try to insert into leaf
        BTreePage leafPage = traversal.leafPage;

        if (!leafPage.isFull()) {
            // Space available - insert directly
            try {
                leafPage.insertKeyPair(key, recordAddress);
                writeBTreePage(leafPage);
                return 0;  // Success
            } catch (RuntimeException e) {
                if (e.getMessage().contains("already exists")) {
                    return -2;  // Duplicate
                }
                throw e;
            }
        }

        // Step 4: Leaf is full - try compensation
        int compensationResult = tryCompensation(key, recordAddress, leafPage,
            traversal.parentPageNumber,
            traversal.positionInParent);

        if (compensationResult >= 0) {
            return compensationResult;  // Success
        }

        // Step 5: Compensation failed - needs splitting (future phase)
        return -1;
    }

    /**
     * Traverse tree from root to find leaf page where key should go
     * Returns TraversalResult with leaf page and parent info
     * Returns null if key already exists
     */
    private TraversalResult findLeaf(int key) throws IOException {
        int currentPageNumber = rootPageNumber;
        int parentPageNumber = -1;
        int positionInParent = -1;

        while (true) {
            BTreePage currentPage = readBTreePage(currentPageNumber);
            SearchResult result = searchInPage(currentPage, key);
            if (result.recordAddress != -1) {
                return null; // // Duplicate found
            }

            if (currentPage.isLeaf()) {
                // Found the leaf
                return new TraversalResult(currentPage, parentPageNumber, positionInParent);
            }

            // Internal node - find which child to follow
            try {
                int childIndex = currentPage.findChildPointer(key);
                parentPageNumber = currentPageNumber;
                positionInParent = childIndex;
                currentPageNumber = currentPage.childrenPages[childIndex];
            } catch (RuntimeException e) {
                if (e.getMessage().contains("already exists")) {
                    return null;  // Duplicate found
                }
                throw e;
            }
        }
    }

    /**
     * Try to compensate (rebalance) with a sibling
     * Returns: parent page number if success, -1 if both siblings full or no siblings exist
     */
    private int tryCompensation(int key, int recordAddress, BTreePage overflowPage,
                                int parentPageNumber, int positionInOverflow) throws IOException {

        // Cannot compensate if overflow page is root (no parent)
        if (parentPageNumber == -1) {
            return -1;  // Needs splitting
        }

        BTreePage parentPage = readBTreePage(parentPageNumber);

        // Try left sibling first
        if (positionInOverflow > 0) {
            int leftSiblingPageNum = parentPage.childrenPages[positionInOverflow - 1];
            BTreePage leftSibling = readBTreePage(leftSiblingPageNum);

            if (!leftSibling.isFull()) {
                // Compensation possible with left sibling
                return performCompensation(key, recordAddress, overflowPage, leftSibling,
                    parentPage, positionInOverflow - 1, true);
            }
        }

        // Try right sibling
        if (positionInOverflow < parentPage.getKeyCount()) {
            int rightSiblingPageNum = parentPage.childrenPages[positionInOverflow + 1];
            BTreePage rightSibling = readBTreePage(rightSiblingPageNum);

            if (!rightSibling.isFull()) {
                // Compensation possible with right sibling
                return performCompensation(key, recordAddress, overflowPage, rightSibling,
                    parentPage, positionInOverflow, false);
            }
        }

        // Both siblings full or no siblings exist
        return -1;  // Needs splitting
    }

    /**
     * Perform compensation (rebalancing) between overflow page and sibling
     * isLeftSibling: true if compensating with left sibling, false for right
     * Returns: parent page number
     */
    private int performCompensation(int key, int recordAddress, BTreePage overflowPage,
                                    BTreePage siblingPage, BTreePage parentPage,
                                    int siblingIndex, boolean isLeftSibling) throws IOException {

        // Determine which is overflow and which is destination
        BTreePage destPage = isLeftSibling ? siblingPage : overflowPage;
        BTreePage srcPage = isLeftSibling ? overflowPage : siblingPage;
        int separatorKeyIndex = isLeftSibling ? siblingIndex : siblingIndex;

        // Gather all keys: srcPage + destPage + separator from parent
        int totalKeys = srcPage.getKeyCount() + destPage.getKeyCount() + 1;
        KeyPair[] allKeys = new KeyPair[totalKeys];

        int idx = 0;

        // Add keys from src page
        for (int i = 0; i < srcPage.getKeyCount(); i++) {
            allKeys[idx++] = new KeyPair(srcPage.keyPairs[i].key, srcPage.keyPairs[i].record_address);
        }

        // Add separator key from parent
        allKeys[idx++] = new KeyPair(parentPage.keyPairs[separatorKeyIndex].key,
            parentPage.keyPairs[separatorKeyIndex].record_address);

        // Add keys from dest page
        for (int i = 0; i < destPage.getKeyCount(); i++) {
            allKeys[idx++] = new KeyPair(destPage.keyPairs[i].key, destPage.keyPairs[i].record_address);
        }

        // Find median index (lower median)
        int medianIndex = totalKeys / 2;
        KeyPair medianKey = allKeys[medianIndex];

        // Clear both pages
        for (int i = 0; i < srcPage.keyPairs.length; i++) {
            srcPage.keyPairs[i] = new KeyPair();
        }
        for (int i = 0; i < destPage.keyPairs.length; i++) {
            destPage.keyPairs[i] = new KeyPair();
        }

        // Distribute keys: first half to srcPage, second half to destPage
        srcPage.keyCount = 0;
        for (int i = 0; i < medianIndex; i++) {
            srcPage.keyPairs[i] = new KeyPair(allKeys[i].key, allKeys[i].record_address);
            srcPage.keyCount++;
        }

        destPage.keyCount = 0;
        for (int i = medianIndex + 1; i < totalKeys; i++) {
            destPage.keyPairs[i - medianIndex - 1] = new KeyPair(allKeys[i].key, allKeys[i].record_address);
            destPage.keyCount++;
        }

        // Replace separator key in parent
        parentPage.keyPairs[separatorKeyIndex] = new KeyPair(medianKey.key, medianKey.record_address);

        // Now try to insert the new key into destPage (which now has space)
        try {
            destPage.insertKeyPair(key, recordAddress);
        } catch (RuntimeException e) {
            if (e.getMessage().contains("already exists")) {
                throw new RuntimeException("Key already exists: " + key);
            }
            throw e;
        }

        // Write all three pages to disk (one by one with flush)
        writeBTreePage(srcPage);
        disk.flush();
        writeBTreePage(destPage);
        disk.flush();
        writeBTreePage(parentPage);
        disk.flush();

        return parentPage.address;  // Success
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

    /**
     * Get a record by key using BTree search algorithm
     */
    public Record getRecord(int key) throws IOException {
        if (rootPageNumber == -1) {
            return null;
        }

        TraversalResult t_result = findLeaf(key);
        if (t_result != null) {
            SearchResult result = searchInPage(t_result.leafPage, key);
            if (result.recordAddress >= 0) {
                return disk.readRecord(Main.File.RECORDS, result.recordAddress);
            }
        }

        return null;
    }

    /**
     * Search for a key within a single page (used by getRecord)
     * Uses binary search
     */
    private SearchResult searchInPage(BTreePage page, int key) {
        int left = 0;
        int right = page.getKeyCount() - 1;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (page.keyPairs[mid].key == key) {
                // Found!
                return new SearchResult(page.keyPairs[mid].record_address, -1);
            } else if (page.keyPairs[mid].key < key) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        // Not found - left is the child index to follow
        return new SearchResult(-1, page.childrenPages[left]);
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

    /**
     * Read a BTree page directly from disk for display purposes
     * WITHOUT incrementing disk read/write counters
     * Returns null if page doesn't exist
     */
    private BTreePage readBTreePageForDisplay(int pageNumber) throws IOException {
        // bypasses caching
        RandomAccessFile raf = new RandomAccessFile(DiskDrive.getFileFromFileType(Main.File.BTREE), "r");

        try {
            long filePosition = (long) pageNumber * 56;  // 56 bytes per page for order=2

            // Check if page is beyond file size
            if (filePosition + 56 > raf.length()) {
                return null;  // Page doesn't exist yet
            }

            raf.seek(filePosition);
            byte[] pageData = new byte[56];
            int bytesRead = raf.read(pageData);

            if (bytesRead != 56) {
                return null;  // Failed to read complete page
            }

            // Deserialize
            BTreePage page = new BTreePage(order, -1);
            page.address = pageNumber;
            page.deserialize(pageData);
            return page;
        } finally {
            raf.close();
        }
    }

    /**
     * Display the complete BTree structure in human-readable format
     * Does NOT increment disk read/write counters
     */
    public void displayTreeStructure() throws IOException {
        DiskDrive.DiskStats STATS_BEFORE_DISPLAY = getDiskStats();
        disk.flush();

        if (rootPageNumber == -1) {
            System.out.println("BTree is empty (no root page)");
            return;
        }

        System.out.println("=== BTree Structure (Order=" + order + ", Max " + (2 * order) + " keys per page) ===\n");

        // Use BFS to organize pages by level
        java.util.Queue<Integer> currentLevel = new java.util.LinkedList<>();
        java.util.Queue<Integer> nextLevel = new java.util.LinkedList<>();
        currentLevel.add(rootPageNumber);

        int level = 0;
        int totalPages = 0;

        while (!currentLevel.isEmpty()) {
            System.out.println("Level " + level + ":");

            while (!currentLevel.isEmpty()) {
                int pageNum = currentLevel.poll();
                BTreePage page = readBTreePageForDisplay(pageNum);

                if (page == null) {
                    continue;
                }

                totalPages++;

                // Format: Page X [keys: ...] [children: ...] [leaf: true/false] [parent: Y]
                StringBuilder sb = new StringBuilder();
                sb.append("  Page ").append(page.address);

                // Keys
                sb.append(" [keys: ");
                for (int i = 0; i < page.getKeyCount(); i++) {
                    if (i > 0) sb.append(", ");
                    sb.append(page.keyPairs[i].key);
                }
                sb.append("]");

                // Children pointers
                sb.append(" [children: ");
                if (page.isLeaf()) {
                    sb.append("none");
                } else {
                    for (int i = 0; i <= page.getKeyCount(); i++) {
                        if (i > 0) sb.append(", ");
                        sb.append(page.childrenPages[i]);
                    }
                }
                sb.append("]");

                // Leaf flag
                sb.append(" [leaf: ").append(page.isLeaf()).append("]");

                // Parent address
                sb.append(" [parent: ").append(page.parentAddress).append("]");

                System.out.println(sb.toString());

                // Add children to next level (if not a leaf)
                if (!page.isLeaf()) {
                    for (int i = 0; i <= page.getKeyCount(); i++) {
                        if (page.childrenPages[i] != -1) {
                            nextLevel.add(page.childrenPages[i]);
                        }
                    }
                }
            }

            // Move to next level
            currentLevel = nextLevel;
            nextLevel = new java.util.LinkedList<>();

            if (!currentLevel.isEmpty()) {
                System.out.println();
            }

            level++;
        }

        System.out.println("\nTotal Pages: " + totalPages);

        disk.restoreStats(STATS_BEFORE_DISPLAY);
    }
}


