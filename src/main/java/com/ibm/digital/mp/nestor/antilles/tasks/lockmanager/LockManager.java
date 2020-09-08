package com.ibm.digital.mp.nestor.antilles.tasks.lockmanager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LockManager
{
    public static final String LOCK_COLLECTION_NAME = "nestorLocks";

    private static Logger logger = LogManager.getLogger(LockManager.class.getName());

    public LockManager()
    {
        //
    }

    /**
     * Obtain distributed lock for given mutex. It will use Mongo for distributed locking.
     * 
     * @param mutex
     *            Mutex which should be locked.
     * @return Lock object, if lock status should be checked using API lock.isAcquired()
     */
    public Lock getLock(String mutex)
    {
        LockGuard guard = getLockGuard();
        try
        {
            return guard.getLock(mutex);
        }
        catch (Throwable ex)
        {
            // Error logging
            logger.error(ex.getMessage(), ex);
        }
        // Error in getting lock
        // Return generic lock
        return new GenericLock(mutex, guard);
    }

    protected LockGuard getLockGuard()
    {
        return new MongoLockGuard();
    }
}
