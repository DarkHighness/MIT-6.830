package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private static class PageSlot {
        Page page;
        TransactionId transactionId;

        final ReentrantReadWriteLock lock;

        public PageSlot() {
            this.lock = new ReentrantReadWriteLock();
        }
    }

    private final int numPages;
    private Map<PageId, Page> pageMap;
    private LinkedList<PageId> lru;
    private LockManager lock;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.pageMap = new ConcurrentHashMap<>();
        this.lru = new LinkedList<>();
        this.lock = new LockManager();
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
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        try {
            if (perm == Permissions.READ_ONLY)
                lock.acquireLock(tid, pid, LockManager.LockType.READ);
            else
                lock.acquireLock(tid, pid, LockManager.LockType.WRITE);
        } catch (InterruptedException ex) {
            throw new DbException(ex.getMessage());
        }

        if (!pageMap.containsKey(pid)) {
            final DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());

            if (pageMap.size() >= numPages)
                evictPage();

            pageMap.put(pid, dbFile.readPage(pid));

            lru.addFirst(pid);
        }

        lru.remove(pid);
        lru.addFirst(pid);

        return pageMap.get(pid);
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        lock.releaseLock(tid, pid, LockManager.LockType.READ);
        lock.releaseLock(tid, pid, LockManager.LockType.WRITE);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId pid) {
        return lock.holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        List<PageId> pageIds = lock.getPageId(tid);

       try {
           for(PageId pid : pageMap.keySet()) {
               if (tid.equals(pageMap.get(pid).isDirty())) {
                   if (commit) {
                       pageMap.get(pid).setBeforeImage();
                       flushPage(pid);
                   } else {
                       discardPage(pid);
                   }
               }
           }
       } catch (NullPointerException | IOException ex) {
           throw new RuntimeException(ex.getMessage());
       }

       for (PageId pid: pageIds)
           unsafeReleasePage(tid, pid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = file.insertTuple(tid, t);

        updateDirtyPages(tid, pages);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = file.deleteTuple(tid, t);

        updateDirtyPages(tid, pages);
    }

    private void updateDirtyPages(TransactionId tid, List<Page> pages) {
        for (Page page: pages) {
            page.markDirty(true, tid);

            PageId pid = page.getId();

            if (!pageMap.containsKey(pid)) {
                if (pageMap.size() >= numPages) {
                    try {
                        evictPage();
                    } catch (DbException ex) {
                        throw new RuntimeException(ex.getMessage());
                    }
                }

                pageMap.put(pid, page);
                lru.addFirst(page.getId());
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (PageId pid : pageMap.keySet())
            flushPage(pid);
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        if (pageMap.containsKey(pid)) {
            pageMap.remove(pid);
            lru.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if (pageMap.containsKey(pid)) {
            Page page = pageMap.get(pid);

            TransactionId tid = page.isDirty();

            if (tid != null) {
                Database.getLogFile().logWrite(tid, page.getBeforeImage(), page);
                Database.getLogFile().force();

                DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                dbFile.writePage(page);

                pageMap.remove(pid);
                lru.remove(pid);

                page.markDirty(false, null);
                page.setBeforeImage();
            }
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for (Map.Entry<PageId, Page> entry: pageMap.entrySet()) {
            PageId pid = entry.getKey();
            Page page = entry.getValue();

            if (tid.equals(page.isDirty()))
                flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        for (PageId pageId : lru) {
            Page   p   = pageMap.get(pageId);
            if (p.isDirty() == null) {
                try {
                    flushPage(pageId);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                discardPage(pageId);
                return;
            }
        }

        throw new DbException("evict failed");
    }
}
