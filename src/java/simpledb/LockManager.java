package simpledb;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

enum LockType {
    SHARED,
    EXCLUSIVE,
    ANY
}

public class LockManager {
    private ConcurrentHashMap<PageId, HashSet<TransactionId>> sharedLocks;
    private ConcurrentHashMap<PageId, TransactionId> exclusiveLocks;
    private ConcurrentHashMap<TransactionId, HashSet<TransactionId>> lockTracker;

    public LockManager () {
        this.sharedLocks = new ConcurrentHashMap<>();
        this.exclusiveLocks = new ConcurrentHashMap<>();
        this.lockTracker = new ConcurrentHashMap<>();
    }

    public synchronized  void getLock(PageId pid, TransactionId tid, Permissions permissions) throws TransactionAbortedException {
        if (permissions.equals(Permissions.READ_WRITE)) {
            // if we have an exclusive lock on the current page
            if (holdsLock(pid, tid, LockType.EXCLUSIVE)) {
                return;
            }

            // there's a exclusive lock on the page, wait till the lock is released
            waitBecauseExclusiveLock(pid, tid);

            // we don't have exclusive lock on this page, we need to acquire it

            if (sharedLockOnPage(pid)) {
                if (!upgradeLock(pid, tid)) {
                    waitBecauseSharedLock(pid, tid);
                }
            }

            // we don't have a lock on this page and we are allowed to get an exclusive lock
            this.exclusiveLocks.put(pid, tid);

            // we acquired the lock, remove traces of tid from lock tracker
            removeFromLockTracker(tid);
        } else { // Permissions.READ_ONLY

            if (holdsLock(pid, tid, LockType.ANY)) {
                return;
            }

            waitBecauseExclusiveLock(pid, tid);

            // we don't have exclusive lock on this page, we can acquire shared lock

            if (sharedLockOnPage(pid)) {
                // this page already holds some shared locks so we add our tid to the existing hash set
                this.sharedLocks.get(pid).add(tid);
            } else {
                // this page does not contain any shared lock, we initialise hash set
                HashSet<TransactionId> sharedTid = new HashSet<>();
                sharedTid.add(tid);
                this.sharedLocks.put(pid, sharedTid);
            }

            // shared lock acquired
            // we can remove tid from lock tracker
            removeFromLockTracker(tid);
        }
    }

    private void waitBecauseSharedLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        // add the given tid's dependency on the tids that holds the shared lock to the lock
        addDependencies(tid, this.sharedLocks.get(pid));

        try {
            wait();
            if (sharedLockOnPage(pid)) {
                if (!upgradeLock(pid, tid)) {
                    waitBecauseSharedLock(pid, tid);
                    waitBecauseExclusiveLock(pid, tid);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void waitBecauseExclusiveLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        if (exclusiveLockOnPage(pid)) {
            addDependencies(tid, this.exclusiveLocks.get(pid));

            try {
                while (exclusiveLockOnPage(pid)) {
                    wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /** Return true if the specified pid is shared locked */
    private synchronized boolean sharedLockOnPage(PageId pid) {
        return this.sharedLocks.containsKey(pid);
    }

    /** Return true if the specified pid is exclusive locked */
    private synchronized boolean exclusiveLockOnPage(PageId pid) {
        return this.exclusiveLocks.containsKey(pid);
    }

    private synchronized boolean upgradeLock(PageId pid, TransactionId tid) {
        if (this.sharedLocks.get(pid).size() == 1 && this.sharedLocks.get(pid).contains(tid)) {
            this.exclusiveLocks.remove(pid);
            return true;
        }
        return false;
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public synchronized boolean holdsLock(PageId pid, TransactionId tid, LockType lock) {
        if (lock == LockType.ANY) {
            return (sharedLockOnPage(pid) && this.sharedLocks.get(pid).contains(tid)) ||
                    (exclusiveLockOnPage(pid) && this.exclusiveLocks.get(pid).equals(tid));
        }

        if (lock == LockType.SHARED) {
            return sharedLockOnPage(pid) && this.sharedLocks.get(pid).contains(tid);
        }

        if (lock == LockType.EXCLUSIVE) {
            return exclusiveLockOnPage(pid) && this.exclusiveLocks.get(pid).equals(tid);
        }

        return false;
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
        // find all pages that the given tid holds a shared lock on
        for (PageId pid : this.sharedLocks.keySet()) {
            // if the tid has a shared lock on the given page, we have to release it
            if (holdsLock(pid, tid, LockType.SHARED)) {
                this.sharedLocks.get(pid).remove(tid);
                if (this.sharedLocks.get(pid).size() == 0) {
                    this.sharedLocks.remove(pid);
                }
            }
        }

        // find pages that the tid has an exclusive lock on
        for (PageId pid : this.exclusiveLocks.keySet()) {
            // if the tid has an exclusive lock on the given page, we have release it
            if (holdsLock(pid, tid, LockType.EXCLUSIVE)) {
                this.exclusiveLocks.remove(pid);
            }
        }
        notifyAll();
    }

    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        // if the tid has a shared lock on pid, we need to release it
        if (holdsLock(pid, tid, LockType.SHARED)) {
            this.sharedLocks.get(pid).remove(tid);
            if (this.sharedLocks.get(pid).size() == 0) {
                this.sharedLocks.remove(pid);
            }
        }

        // if the tid has an exclusive lock on the pid, we need to release it
        if (holdsLock(pid, tid, LockType.EXCLUSIVE)) {
            this.exclusiveLocks.remove(pid);
        }
        notifyAll();
    }

    public synchronized void addDependencies(TransactionId tid1, TransactionId tid2) {
        // ensure that tid1 does not depend on itself
        if (tid1.equals(tid2)) {
            return;
        }

        if (this.lockTracker.containsKey(tid1)) {
            this.lockTracker.get(tid1).add(tid2);
        } else {
            this.lockTracker.put(tid1, new HashSet<>());
            this.lockTracker.get(tid1).add(tid2); // insyallah
        }
    }

    public synchronized void addDependencies(TransactionId tid, HashSet<TransactionId> sharedTid) {
        HashSet<TransactionId> lockQueue;
        // fetch all tids that "tid" relies on
        if (!this.lockTracker.containsKey(tid)) {
            lockQueue = new HashSet<>();
        } else {
            lockQueue = this.lockTracker.get(tid);
        }

        // in case 'tid'  is inside the set of 'sharedTid'
        // we ensure 'tid' doesn't depend on itself
        HashSet<TransactionId> dependencies = new HashSet<TransactionId>(sharedTid);
        dependencies.remove(tid);

        // add all tids in 'sharedTid', so that they are dependencies for the given 'tid'
        lockQueue.addAll(dependencies);

        this.lockTracker.put(tid, lockQueue);
    }

    private synchronized void removeFromLockTracker(TransactionId tid) {
        this.lockTracker.remove(tid);
        for (TransactionId i : this.lockTracker.keySet()) {
            this.lockTracker.get(i).remove(tid);
        }
    }


}
