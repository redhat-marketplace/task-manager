package com.ibm.digital.mp.nestor.antilles.tasks.lockmanager;

import org.junit.Assert;
import org.junit.Test;

public class GenericLockTest
{
    @Test
    public void testUnlockWhenNoLockAcquired()
    {
        Lock lock = new GenericLock("mutex", null);
        Assert.assertFalse(lock.unlock());
    }

    @Test
    public void testUnlock()
    {
        LockGuard guard = new LockGuard()
        {
            @Override
            public Lock getLock(String mutex)
            {
                return new GenericLock(mutex, this);
            }

            @Override
            public boolean unlock(Lock lock)
            {
                return true;
            }
        };
        GenericLock lock = new GenericLock("mutex", guard);
        lock.setIsAcquired(true);
        Assert.assertTrue(lock.unlock());
        Assert.assertFalse(lock.isAcquired());
    }

    @Test
    public void testUnlockFalse()
    {
        LockGuard guard = new LockGuard()
        {
            @Override
            public Lock getLock(String mutex)
            {
                return new GenericLock(mutex, this);
            }

            @Override
            public boolean unlock(Lock lock)
            {
                return false;
            }
        };
        GenericLock lock = new GenericLock("mutex", guard);
        lock.setIsAcquired(true);
        Assert.assertFalse(lock.unlock());
        Assert.assertTrue(lock.isAcquired());
    }
}
