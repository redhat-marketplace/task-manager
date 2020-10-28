package com.ibm.digital.mp.nestor.antilles.tasks.lockmanager;

public class GenericLock implements Lock
{
    private String mutex = null;
    
    private boolean acquired = false;
    
    private LockGuard guard = null;
    
    protected GenericLock(String mutex, LockGuard guard)
    {
        this.mutex = mutex;
        this.guard = guard;
    }
    
    @Override
    public String getMutex()
    {
        return this.mutex;
    }
    
    protected void setIsAcquired(boolean acquired)
    {
        this.acquired = acquired;
    }
    
    @Override
    public boolean isAcquired()
    {
        return this.acquired;
    }
    
    @Override
    public boolean unlock()
    {
        if (!acquired)
        {
            // No lock was acquired
            // Nothing for unlock
            return false;
        }
        boolean unlocked = guard.unlock(this);
        setIsAcquired(!unlocked);
        return unlocked;
    }
}