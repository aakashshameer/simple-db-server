package simpledb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
     other classes. BufferPool should use the numPages argument to the
     constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private ConcurrentHashMap<PageId, Page> pool;
    private int numPages;
    private LockManager lockManager;


    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.pool = new ConcurrentHashMap<>();
        this.numPages = numPages;
        this.lockManager = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here

        this.lockManager.getLock(pid, tid, perm);

        if (!this.pool.containsKey(pid)) {
            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);

            // if pool size is greater or equal than numPages, we need to evict
            this.evictPage();

            this.pool.put(pid, page);
        }
        return this.pool.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.lockManager.releaseLock(pid, tid);
    }

    /**
     * Checks if a specified transaction id holds a lock on a specified page
     *
     * @param tid the ID of the transaction we want to check for locks
     * @param pid the page id we want to check for locks
     * @return true if tid holds some type of lock on pid
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        return this.lockManager.holdsLock(pid, tid, LockType.ANY);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        this.transactionComplete(tid, true);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

        // check if we need to commit or abort
        if (!commit) {
            // abort
            for (PageId pid: this.pool.keySet()) {
                TransactionId currTid = this.pool.get(pid).isDirty();
                // we discard any dirty pages so that the dirty changes will not be seen by any other transactions
                if (currTid != null && currTid.equals(tid)) {
                    this.discardPage(pid);
                }
            }
        } else {
            // LAB 3: commit by flushing all pages
            // this.flushPages(tid);
            // LAB 4: NO-FORCE: no longer force pages to disk when committing
            for (Page p: this.pool.values()) {
                // add dirty pages to the log
                Database.getLogFile().logWrite(tid, p.getBeforeImage(),p);
                // force the log to disk
                Database.getLogFile().force();
                // use curr page contents as the before image because the dirty changes cannot be seen
                p.setBeforeImage();
            }
        }
        this.lockManager.releaseAllLocks(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);

        ArrayList<Page> modifiedPage = file.insertTuple(tid, t);
        for (Page page: modifiedPage) {
            page.markDirty(true, tid);
            if (!this.pool.containsKey(page.getId())) {
                this.evictPage();
                this.pool.put(page.getId(), page);
            } else {
                this.pool.put(page.getId(), page);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);

        ArrayList<Page> modifiedPage = file.deleteTuple(tid, t);
        for (Page page: modifiedPage) {
            page.markDirty(true, tid);
            // TODO: Aakash, do a check for containsKey in buffer pool?
            if (!this.pool.containsKey(page.getId())) {
                this.evictPage();
                this.pool.put(page.getId(), page);
            } else {
                this.pool.put(page.getId(), page);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pageId: this.pool.keySet()) {
            this.flushPage(pageId);
        }

    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        // buffer pool does not contain page
        if (!this.pool.containsKey(pid)) {
            return;
        }

        // buffer pool contains page
        Page page = this.pool.get(pid);
        TransactionId dirtier = page.isDirty();
        if (dirtier != null) { // check if page is dirty
            // page is dirty, we want to write it to disk
            HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());

            // check if the transaction that cause this page to be dirty is still executing,
            // if it is, we have to change the log
            if (this.lockManager.holdsLock(pid, dirtier, LockType.ANY)) {
                Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
                Database.getLogFile().force();
            }

            heapFile.writePage(page);
            page.markDirty(false, null);


        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (PageId pid : pool.keySet()) {
            TransactionId currTid = pool.get(pid).isDirty();
            if(currTid != null && currTid.equals(tid)) {
                flushPage(pid);
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
     Needed by the recovery manager to ensure that the
     buffer pool doesn't keep a rolled back page in its
     cache.

     Also used by B+ tree files to ensure that deleted pages
     are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.pool.remove(pid);
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        // if pool.size if lesser that than numPages, we don't need to evict
        if (this.pool.size() < this.numPages) {
            return;
        }

        // get the random page from the buffer pool to evict
        int random = (int) Math.floor(Math.random() * this.pool.size());
        ArrayList<PageId> evictList = new ArrayList<>(this.pool.keySet());

        PageId pid = evictList.get(random);
        Page randomPage = this.pool.get(pid);

        //        we are checking if the random pages are dirty
        //        if pages are dirty, we do not evict
        //        while (randomPage.isDirty() != null) {
        //            if (evictList.isEmpty()) {
        //                throw new DbException("No pages to evict. All pages are dirty");
        //            }
        //            evictList.remove(pid);
        //
        //            if (!evictList.isEmpty()) {
        //                random = (int) Math.floor(Math.random() * evictList.size());
        //                pid = evictList.get(random);
        //                randomPage = this.pool.get(pid);
        //            }
        //        }

        try {
            this.flushPage(pid);
        } catch (IOException e) {
            throw new DbException("Page flush during eviction failed.");
        }
        this.pool.remove(pid);
    }

}