package com.ibm.digital.mp.nestor.antilles.tasks.lockmanager;

import org.junit.Assert;
import org.junit.Test;

public class LockManagerTest
{
    @Test
    public void testMongoLocking()
    {
        LockManager mgr = new LockManager();
        // Mongo locking has hard dependancy on the Mongo db connection, hence for now lock can not be obtained in junit tests
        Lock lock = mgr.getLock("123");
        Assert.assertFalse(lock.isAcquired());
    }
    
    @Test
    public void testMongoLockingWhenThrowableErrorInGetLock()
    {
        LockManager mgr = new LockManager()
        {
            @Override
            protected LockGuard getLockGuard()
            {
                return new MongoLockGuard()
                {
                    @Override
                    public Lock getLock(String mutex)
                    {
                        throw new RuntimeException("Error in locking");
                    }
                };
            }
        };
        Lock lock = mgr.getLock("123");
        Assert.assertFalse(lock.isAcquired());
    }
}