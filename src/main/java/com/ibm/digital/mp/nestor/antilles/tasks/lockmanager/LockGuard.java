package com.ibm.digital.mp.nestor.antilles.tasks.lockmanager;

public interface LockGuard
{
    public Lock getLock(String mutex);
    
    public boolean unlock(Lock lock);
}
