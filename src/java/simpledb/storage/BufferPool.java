package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

class LockManager {
    //private Map<PageId, ReentrantReadWriteLock> lockMap;
    private Map<PageId, Set<TransactionId>> readTidMap;
    private Map<PageId, TransactionId> writeTidMap;
    private Map<TransactionId, Set<PageId>> tidPageMap;

    public LockManager() {
        //lockMap = new ConcurrentHashMap<>();
        readTidMap = new ConcurrentHashMap<>();
        writeTidMap = new ConcurrentHashMap<>();
        tidPageMap = new ConcurrentHashMap<>();
    }

    public synchronized void putPageId2TidMap(PageId pageId, TransactionId tid) {
        //if (!tidPageMap.containsKey(tid)) {
        //    tidPageMap.put(tid, new HashSet<>());
        //}
        tidPageMap.get(tid).add(pageId);
    }

    public synchronized void removePageIdFromTidMap(PageId pageId, TransactionId tid) {
        tidPageMap.get(tid).remove(pageId);
    }

    public synchronized boolean lock(PageId pageId, TransactionId tid, boolean sharedLock) throws InterruptedException {
        //Random random = new Random();
        //long randomTimeout = random.nextInt(10);
        if (!tidPageMap.containsKey(tid)) {
            tidPageMap.put(tid, new HashSet<>());
        }
        if (sharedLock) {
            //申请共享锁
            if (writeTidMap.containsKey(pageId)) {
                //有事务占有排他锁，判断该事务是否是自己
                if (tid.equals(writeTidMap.get(pageId))) {
                    //System.out.println(String.format("Transaction[%d] get readLock of page(%d-%d)", tid.getId(), pageId.getPageNumber(), pageId.getTableId()));
                    return true;
                }
                System.out.println(String.format("Transaction[%d] cannot get readLock of page(%d-%d), because other tid has exclusive lock",
                        tid.getId(), pageId.getPageNumber(), pageId.getTableId()));
                //wait(randomTimeout);
                //wait(5);
                return false;
            }
            //没有事务持有排他锁，可以申请共享锁
            if (!readTidMap.containsKey(pageId)) {
                //没有事务持有共享锁
                readTidMap.put(pageId, new HashSet<>());
                readTidMap.get(pageId).add(tid);
                putPageId2TidMap(pageId, tid);
                System.out.println(String.format("Transaction[%d] get readLock of page(%d-%d)", tid.getId(), pageId.getPageNumber(), pageId.getTableId()));
            } else {
                //检查tid是否已经持有pageId的共享锁
                if (!readTidMap.get(pageId).contains(tid)) {
                    putPageId2TidMap(pageId, tid);
                    readTidMap.get(pageId).add(tid);
                    System.out.println(String.format("Transaction[%d] get readLock of page(%d-%d)", tid.getId(), pageId.getPageNumber(), pageId.getTableId()));
                }
            }
            return true;
        } else {
            //申请排他锁
            if (!writeTidMap.containsKey(pageId)) {
                //没有事务持有排他锁
                if (readTidMap.get(pageId) == null || (readTidMap.get(pageId).contains(tid) && readTidMap.get(pageId).size() == 1)) {
                    //没有事务持有共享锁或者只有tid持有共享锁
                    writeTidMap.put(pageId, tid);
                    readTidMap.remove(pageId);
                    putPageId2TidMap(pageId, tid);
                    System.out.println(String.format("Transaction[%d] get writeLock of page(%d-%d)", tid.getId(), pageId.getPageNumber(), pageId.getTableId()));
                    return true;
                } else {
                    System.out.println(String.format("Transaction[%d] cannot get writeLock of page(%d-%d) because other tid has shared lock", tid.getId(), pageId.getPageNumber(), pageId.getTableId()));
                    //wait(5);
                    return false;
                }
            } else {
                if (tid.equals(writeTidMap.get(pageId))) {
                    writeTidMap.put(pageId, tid);
                    putPageId2TidMap(pageId, tid);
                    //System.out.println(String.format("Transaction[%d] get writeLock of page(%d-%d)", tid.getId(), pageId.getPageNumber(), pageId.getTableId()));
                    return true;
                }
                System.out.println(String.format("Transaction[%d] cannot get writeLock of page(%d-%d) because other tid has exclusive lock", tid.getId(), pageId.getPageNumber(), pageId.getTableId()));
                //wait(5);
                return false;
            }
        }
    }

    public synchronized void unlock(PageId pageId, TransactionId tid) {
        if (writeTidMap.containsKey(pageId)) {
            if (writeTidMap.get(pageId).equals(tid)) {
                System.out.println(String.format("Transaction[%d] unlock writeLock of page(%d-%d)", tid.getId(), pageId.getPageNumber(), pageId.getTableId()));
                writeTidMap.remove(pageId);
                removePageIdFromTidMap(pageId, tid);
            }
        }
        if (readTidMap.containsKey(pageId)) {
            Set<TransactionId> transactionIdSet = readTidMap.get(pageId);
            if (transactionIdSet.contains(tid)) {
                System.out.println(String.format("Transaction[%d] unlock readLock of page(%d-%d)", tid.getId(), pageId.getPageNumber(), pageId.getTableId()));
                removePageIdFromTidMap(pageId, tid);
                transactionIdSet.remove(tid);
            }
            if (transactionIdSet.size() == 0) {
                readTidMap.remove(pageId);
            }
        }
        //notifyAll();
    }

    public synchronized boolean holdsLock(PageId p, TransactionId tid) {
        return writeTidMap.get(p).equals(tid) ||
                (readTidMap.containsKey(p) && readTidMap.get(p).contains(tid));
    }

    public synchronized List<PageId> getTidPages(TransactionId tid) {
        if(tidPageMap.containsKey(tid)){
            return tidPageMap.get(tid).stream().toList();
        }else{
            return new LinkedList<>();
        }
    }

    public synchronized void close(TransactionId tid) {
        List<PageId> tidPages = getTidPages(tid);
        for (PageId pageId : tidPages) {
            //try {
            unlock(pageId, tid);
            //} catch (TransactionAbortedException e) {
            //    e.printStackTrace();
            //}
        }
        //tidPageMap.remove(tid);
    }
}

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

    private Logger logger = Logger.getLogger(BufferPool.class.getName());
    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private static Map<PageId, Page> bufferPool;    // 由pageId到Page的映射
    private static int numPages;   // bufferPool的页面数
    private LockManager lockManager;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        bufferPool = new LinkedHashMap<>();
        lockManager = new LockManager();

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
        // some code goes here
        //在返回Page之前加锁
        long t1 = System.currentTimeMillis();
        boolean sharedLock = perm == Permissions.READ_ONLY;
        Random random = new Random(System.currentTimeMillis() + Thread.currentThread().getName().hashCode());
        long randomTimeout = random.nextInt(300) + 200;
        while (true) {
            try {
                if (lockManager.lock(pid, tid, sharedLock)) {
                    break;
                }
                long now = System.currentTimeMillis();
                try {
                    Thread.sleep(randomTimeout);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (lockManager.lock(pid, tid, sharedLock)) {
                    break;
                }
                //if (now - t1 > randomTimeout) {
                System.out.println(String.format("Transaction[%d] may have dead lock, aborted", tid.getId()));
                throw new TransactionAbortedException();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            //}
        }
        //while (true) {
        //    try {
        //        if (lockManager.lock(pid, tid, sharedLock)) {
        //            break;
        //        }
        //    } catch (InterruptedException e) {
        //        e.printStackTrace();
        //    }
        //    long now = System.currentTimeMillis();
        //    if (now - t1 > 500) {
        //        System.out.println(String.format("Transaction[%d] may have dead lock, aborted", tid.getId()));
        //        throw new TransactionAbortedException();
        //    }
        //    Random random = new Random();
        //    long randomTimeout = random.nextInt(100);
        //    try {
        //        Thread.sleep(randomTimeout);
        //    } catch (InterruptedException e) {
        //        e.printStackTrace();
        //    }
        //}
        Page page = bufferPool.get(pid);
        if (page == null) {
            //当前页面不在bufferPool中，需要从DbFile中读取这页面。
            if (bufferPool.size() >= numPages) {
                //throw new DbException("缓冲池已满，暂时没有替换策略");  // 暂时抛出
                evictPage();//使用lru算法进行置换
            }
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = dbFile.readPage(pid);
            page.markDirty(false, tid);//新读取到的page设置为false
            bufferPool.put(pid, page);
        } else {
            //更新page为最新(lru算法)
            bufferPool.remove(pid);
            bufferPool.put(pid, page);
        }
        //if (perm == Permissions.READ_ONLY) {
        //    lockManager.getReadLock(pid, tid);
        //} else if (perm == Permissions.READ_WRITE) {
        //    lockManager.getWriteLock(pid, tid);
        //}
        return page;
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
        // some code goes here
        // not necessary for lab1|lab2
        //try {
        //lockManager.unlock(tid, pid);
        lockManager.unlock(pid, tid);
        //} catch (TransactionAbortedException e) {
        //    e.printStackTrace();
        //}
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2

        //return false;
        return lockManager.holdsLock(p, tid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            //commit将所有tid涉及的脏页刷新回disk，释放page的锁
            //When you commit, you should flush dirty pages associated to the transaction to disk
            try {
                //flushPages(tid);
                List<PageId> tidPages = lockManager.getTidPages(tid);
                //List<PageId> tidPages = lockManager.getLockList(tid);
                for (PageId pageId : tidPages) {
                    Page page = bufferPool.get(pageId);
                    if (page == null || page.isDirty() == null) continue;
                    flushPage(pageId);
                    page.setBeforeImage();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            //When you abort, you should revert any changes made by the transaction
            //by restoring the page to its on-disk state.
            try {
                reloadPages(tid);
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            } catch (DbException e) {
                e.printStackTrace();
            }
        }
        lockManager.close(tid);
        System.out.println(String.format("Transaction[%d] complete,will %s", tid.getId(),commit?"commit":"rollback"));
        //lockManager.releaseLocksOnTransaction(tid);
    }

    private synchronized void reloadPages(TransactionId tid) throws TransactionAbortedException, DbException {
        List<PageId> tidPages = lockManager.getTidPages(tid);
        //List<PageId> tidPages = lockManager.getLockList(tid);

        for (PageId pageId : tidPages) {
            Page page = getPage(tid, pageId, Permissions.READ_ONLY);
            if (page.isDirty() != null) {
                //reload page
                bufferPool.remove(pageId);
                //try {
                //    getPage(tid,pageId,Permissions.READ_ONLY);
                //} catch (TransactionAbortedException e) {
                //    e.printStackTrace();
                //} catch (DbException e) {
                //    e.printStackTrace();
                //}
            }
        }
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
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> dirtyPages = dbFile.insertTuple(tid, t);
        for (Page dirtyPage : dirtyPages) {
            dirtyPage.markDirty(true,tid);
            bufferPool.put(dirtyPage.getId(), dirtyPage);
        }
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
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> dirtyPages = dbFile.deleteTuple(tid, t);
        System.out.println(String.format("Transaction[%d] delete %s success", tid.getId(),t));
        for (Page dirtyPage : dirtyPages) {
            dirtyPage.markDirty(true, tid);
            bufferPool.put(dirtyPage.getId(), dirtyPage);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        //List<PageId> tidPages = lockManager.getTidPages(tid);
        //List<PageId> tidPages = lockManager.getLockList(tid);
        for (PageId pageId : bufferPool.keySet()) {
            flushPage(pageId);
        }
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
        // some code goes here
        // not necessary for lab1
        bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = bufferPool.get(pid);
        if (page == null) return;
        TransactionId transactionId = page.isDirty();
        if (transactionId == null) return;

        Database.getLogFile().logWrite(transactionId,page.getBeforeImage(),page);
        Database.getLogFile().force();
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        dbFile.writePage(page);
        page.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        List<PageId> tidPages = lockManager.getTidPages(tid);
        //List<PageId> tidPages = lockManager.getLockList(tid);
        for (PageId pageId : tidPages) {
            flushPage(pageId);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        Page page = bufferPool.entrySet().iterator().next().getValue();
        if (page.isDirty() != null) {
            ////脏页，置换前需要flush到disk上
            //try {
            //    flushPage(page.getId());
            //} catch (IOException e) {
            //    e.printStackTrace();
            //}

            //lab4 实现NO STEAL，不可以淘汰脏页
            //寻找一个非脏页进行evict
            Iterator<Page> iterator = bufferPool.values().iterator();
            while (iterator.hasNext()) {
                page = iterator.next();
                if (page.isDirty() == null) {
                    discardPage(page.getId());
                    return;
                }
            }
            throw new DbException("No page that is not dirty can be evicted");
        }
        //删除
        discardPage(page.getId());
    }

}
