package com.ibm.digital.mp.nestor.antilles.tasks.lockmanager;

public interface Lock
{
    public boolean isAcquired();
    
    public boolean unlock();
    
    public String getMutex();
}
