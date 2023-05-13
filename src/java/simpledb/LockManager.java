package simpledb;

import java.util.*;
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

    /**
     * Obtains the lock for a page
     *
     * @param pid page ID of the page to be locked
     * @param tid the Transaction ID that wants to obtain the lock on the page
     * @param permissions the requested permissions for the page
     * @throws TransactionAbortedException when a deadlock occurs
     */
    public synchronized void getLock(PageId pid, TransactionId tid, Permissions permissions) throws TransactionAbortedException {
        if (permissions.equals(Permissions.READ_WRITE)) {
            // if we have an exclusive lock on the current page
            if (holdsLock(pid, tid, LockType.EXCLUSIVE)) {
                return;
            }

            // there's a exclusive lock on the page, wait till the lock is released
            checkBecauseExclusiveLock(pid, tid);

            // we don't have exclusive lock on this page, we need to acquire it

            if (sharedLockOnPage(pid)) {
                if (!upgradeLock(pid, tid)) {
                    checkBecauseSharedLock(pid, tid);
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

            checkBecauseExclusiveLock(pid, tid);

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

    /**
     * Attempts to abort or retry to obtain the lock while waiting
     * due to a shared lock
     *
     * @param pid page ID of the page to be locked
     * @param tid the Transaction ID that we want to check against
     * @throws TransactionAbortedException when a deadlock occurs
     */
    private void checkBecauseSharedLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        // add the given tid's dependency on the tids that holds the shared lock to the lock
        addDependencies(tid, this.sharedLocks.get(pid));

        // add check for deadlock
        if (deadlockDetected(tid)) {
            removeFromLockTracker(tid);
            // if deadlock detected, throw exception
            throw new TransactionAbortedException();
        }

        try {
            wait();
            if (sharedLockOnPage(pid)) {
                if (!upgradeLock(pid, tid)) {
                    checkBecauseSharedLock(pid, tid);
                    checkBecauseExclusiveLock(pid, tid);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Attempts to abort or retry to obtain the lock while waiting
     * due to an exclusive lock
     *
     * @param pid page ID of the page to be locked
     * @param tid the Transaction ID that we want to check against
     * @throws TransactionAbortedException when a deadlock occurs
     */
    private void checkBecauseExclusiveLock(PageId pid, TransactionId tid) throws TransactionAbortedException {
        if (exclusiveLockOnPage(pid)) {
            addDependencies(tid, this.exclusiveLocks.get(pid));

            // add check for deadlock
            if (deadlockDetected(tid)) {
                removeFromLockTracker(tid);
                // if deadlock detected, throw exception
                throw new TransactionAbortedException();
            }

            // no deadlock detected so we wait on the exclusive lock until we can grab it
            try {
                while (exclusiveLockOnPage(pid)) {
                    wait();

                    // add check for deadlock again, because you will never know
                    if (deadlockDetected(tid)) {
                        removeFromLockTracker(tid);
                        // if deadlock detected, throw exception
                        throw new TransactionAbortedException();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Checks and returns true if there is a shared lock on a specified pid;
     * returns false otherwise
     *
     * @param pid page ID of the page to be locked
     * @return true if there is a shared lock on the pid; false otherwise
     */
    private synchronized boolean sharedLockOnPage(PageId pid) {
        return this.sharedLocks.containsKey(pid);
    }

    /**
     * Checks and returns true if there is an exclusive lock on a specified pid;
     * returns false otherwise
     *
     * @param pid page ID of the page to be locked
     * @return true if there is an exclusive lock on the pid; false otherwise
     */
    private synchronized boolean exclusiveLockOnPage(PageId pid) {
        return this.exclusiveLocks.containsKey(pid);
    }

    /**
     * Attempts to upgrade the lock on a page
     *
     * @param pid page ID of the page to be locked
     * @param tid the Transaction ID that holds the lock on the page
     * @return true if lock has been upgraded; false otherwise
     */
    private synchronized boolean upgradeLock(PageId pid, TransactionId tid) {
        if (this.sharedLocks.get(pid).size() == 1 && this.sharedLocks.get(pid).contains(tid)) {
            this.exclusiveLocks.remove(pid);
            return true;
        }
        return false;
    }

    /**
     * Checks if there is some type of lock on a specified pid
     *
     * @param pid page ID of the page to be locked
     * @param tid the Transaction ID we want to check against
     * @param lock the enum type of lock to check for; may be ANY, SHARED, EXCLUSIVE
     * @return true if the tid holds some type of lock on pid; false otherwise
     */
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

    /**
     * Releases all locks that are held by a specified tid for all pages
     *
     * @param tid the Transaction ID whose locks we want to release
     */
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

    /**
     * Releases the lock that is held on by a specified transaction on a specified page
     *
     * @param pid the page ID held by the lock we want to release
     * @param tid the Transaction ID that wants to obtain the lock on the page
     */
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

    /**
     *
     * Adds dependency between two transaction ids
     *
     * @param tid1 first transaction id, tid that depends on tid2 (waiting on tid2)
     * @param tid2 second transaction id, tid that tid1 depends on
     */
    public synchronized void addDependencies(TransactionId tid1, TransactionId tid2) {
        // ensure that tid1 does not depend on itself
        if (tid1.equals(tid2)) {
            return;
        }

        if (this.lockTracker.containsKey(tid1)) {
            this.lockTracker.get(tid1).add(tid2);
        } else {
            this.lockTracker.put(tid1, new HashSet<>());
            this.lockTracker.get(tid1).add(tid2);
        }
    }

    /**
     * Adds dependency between one transaction id and a group of transaction ids
     *
     * @param tid transaction id that depends on sharedTid
     * @param sharedTid group of transaction IDs that param tid depends on
     */
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

    /**
     * Removes all (lock) dependencies associated with a specified transaction id
     *
     * @param tid the transaction id that should be removed from dependency tracker graph
     */
    private synchronized void removeFromLockTracker(TransactionId tid) {
        this.lockTracker.remove(tid);
        for (TransactionId i : this.lockTracker.keySet()) {
            this.lockTracker.get(i).remove(tid);
        }
    }

    /**
     * Checks for a deadlock associated with a specified transaction id
     *
     * @param tid transaction id we want to check for deadlocks
     * @return true if deadlock (cycle) is detected, false otherwise
     */
    private synchronized boolean deadlockDetected(TransactionId tid) {
        // if the specified tid is not in our lock tracker dependency graph
        // that means it does not have any dependencies and thus deadlock is not possible
        if (!this.lockTracker.containsKey(tid)) {
            return false;
        }

        // Keep track of all tids/nodes we still need to check
        Queue<TransactionId> toCheck = new LinkedList<>();

        // add every tid to toCheck
        toCheck.add(tid);

        // Keep track of all the tids/nodes we have checked
        List<TransactionId> alreadyChecked = new ArrayList<>();

        // check for deadlocks for all tids not yet visited
        while (!toCheck.isEmpty()) {
            TransactionId currTid = toCheck.remove();
            alreadyChecked.add(currTid);

            // check if tid has other dependencies, if so, check each of its dependencies
            if (this.lockTracker.containsKey(currTid)) {
                HashSet<TransactionId> dependencies = this.lockTracker.get(currTid);
                for (TransactionId t : dependencies) {
                    // not yet visited, add to the not yet visited queue
                    // because we need to check this node and its dependencies
                    if (!alreadyChecked.contains(t)) {
                        toCheck.add(t);
                    } else {
                        // we already visited this tid_dependency node as well as its dependencies which means
                        // there is a cycle
                        return true;
                    }
                }
            }
            // no dependencies listed, continue to next tid to check from queue
        }
        // no cycle found in dependency graph, so no deadlock
        return false;
    }
}
