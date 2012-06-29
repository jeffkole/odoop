package com.opower.common.reflect;

/**
 * Utility methods for dealing with reflection
 *
 * @author jeff@opower.com
 */
public final class Reflection {
    private Reflection() {}

    /**
     * Determine if the object will be deserializable, meaning it can be instantiated through
     * a public default constructor.  Obviously, if the class definition does not exist in the
     * location of deserialization, then it will not be deserializable.
     *
     * You will notice that the implementation of this method is practically worthless and may
     * question why I would write it at all.  The purpose of this method is to encapsulate the
     * "logic" of the method and to make a clear indication of the purpose of calls to it.
     *
     * @param object the object to determine deserialiability for
     * @throws RuntimeException if the object will likely not be deserializable, the wrapped
     *         exception will hold more details on why
     */
    public static void checkDeserializable(Object object) {
        try {
            object.getClass().newInstance();
        }
        catch (Exception e) {
            throw new RuntimeException("Object is not deserializable", e);
        }
    }
}
