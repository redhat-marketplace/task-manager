package com.ibm.digital.mp.nestor.antilles.util;

import java.lang.reflect.Constructor;

public class TestUtil
{

    /**
     * Utility method to invoke constructor of a class.
     * 
     * @param classObj
     *            - class object.
     * @throws Exception
     *             - can throw exception.
     */
    public static void invokeConstructor(Class classObj) throws Exception
    {
        Constructor constructor = classObj.getDeclaredConstructor();
        constructor.setAccessible(true);
        constructor.newInstance();
    }

}
