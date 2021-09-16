package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    public enum LockType {
        WRITE,
        READ
    }

    class LockablePageId {
        private final PageId pageId;
        private final LockType lockType;

        public LockablePageId(PageId pageId, LockType lockType) {
            this.pageId = pageId;
            this.lockType = lockType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            LockablePageId that = (LockablePageId) o;
            return Objects.equals(pageId, that.pageId) && lockType == that.lockType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(pageId, lockType);
        }
    }

    private LinkedList<TransactionId> waitings;
    private ConcurrentHashMap<LockablePageId, List<TransactionId>> lockMap;
    private Random random;

    public LockManager() {
        this.waitings = new LinkedList<>();
        this.lockMap = new ConcurrentHashMap<>();
        this.random = new Random();
    }

    private synchronized void parkTransaction(TransactionId tid)
            throws InterruptedException, TransactionAbortedException {
        waitings.addLast(tid);

        long eps = random.nextInt(1000) + 1000;
        long waitingStart = System.currentTimeMillis();

        wait(eps);

        long waitingEnd = System.currentTimeMillis();

        if (waitingEnd - waitingStart > eps)
            throw new TransactionAbortedException();
    }

    public synchronized void acquireLock(TransactionId tid, PageId pid, LockType type)
        throws InterruptedException, TransactionAbortedException {
        if (type.equals(LockType.WRITE)) {
            while (true) {
                if (acquireWriteLock(tid, pid))
                    return;
                else
                    parkTransaction(tid);
            }
        } else {
            while (true) {
                if (acquireReadLock(tid, pid))
                    return;
                else
                    parkTransaction(tid);
            }
        }
    }

    public boolean acquireReadLock(TransactionId tid, PageId pid) {
        final LockablePageId readLockPage = new LockablePageId(pid, LockType.READ);
        final LockablePageId writeLockPage = new LockablePageId(pid, LockType.WRITE);

        synchronized (this) {
            if (lockMap.containsKey(writeLockPage)) {
                return lockMap.get(writeLockPage).get(0).equals(tid);
            } else if (lockMap.containsKey(readLockPage)) {
                if (!lockMap.get(readLockPage).contains(tid))
                    lockMap.get(readLockPage).add(tid);
                return true;
            } else {
                List<TransactionId> ids = new ArrayList<>();
                ids.add(tid);

                lockMap.put(readLockPage, ids);

                return true;
            }
        }
    }

    public boolean acquireWriteLock(TransactionId tid, PageId pid) {
        final LockablePageId readLockPage = new LockablePageId(pid, LockType.READ);
        final LockablePageId writeLockPage = new LockablePageId(pid, LockType.WRITE);

        synchronized (this) {
            if (lockMap.containsKey(writeLockPage)) {
                return lockMap.get(writeLockPage).get(0).equals(tid);
            } else if (lockMap.containsKey(readLockPage)) {
               if (lockMap.get(readLockPage).contains(tid))
                   return lockMap.get(readLockPage).size() == 1;
               else
                   return false;
            } else {
                List<TransactionId> ids = new ArrayList<>();
                ids.add(tid);

                lockMap.put(writeLockPage, ids);

                return true;
            }
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid, LockType type) {
        final LockablePageId lockablePageId = new LockablePageId(pid, type);
        List<TransactionId> transactionIds = lockMap.get(lockablePageId);

        if (transactionIds == null)
            return;

        if (transactionIds.size() == 1)
            lockMap.remove(lockablePageId);
        else {
            transactionIds.remove(transactionIds);
            lockMap.put(lockablePageId, transactionIds);
        }

        waitings.remove(tid);

        notifyAll();
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        final LockablePageId readLockPage = new LockablePageId(pid, LockType.READ);
        final LockablePageId writeLockPage = new LockablePageId(pid, LockType.WRITE);

        return (lockMap.containsKey(readLockPage) && lockMap.get(readLockPage).contains(tid))
                || (lockMap.containsKey(writeLockPage) && lockMap.get(writeLockPage).contains(tid));
    }

    public List<PageId> getPageId(TransactionId tid) {
        List<PageId> pageIds = new ArrayList<>();

        for(LockablePageId pageId : lockMap.keySet())
            if (lockMap.get(pageId).contains(tid))
                pageIds.add(pageId.pageId);

        return pageIds;
    }
}
