package com.opower.common.reflect;

import org.junit.Test;

import static com.opower.common.reflect.hide.ReflectionTestClasses.publicStaticClassWithPublicConstructor;
import static com.opower.common.reflect.hide.ReflectionTestClasses.publicStaticClassWithPrivateConstructor;
import static com.opower.common.reflect.hide.ReflectionTestClasses.publicStaticClassWithProtectedConstructor;
import static com.opower.common.reflect.hide.ReflectionTestClasses.publicStaticClassWithNonDefaultConstructor;
import static com.opower.common.reflect.hide.ReflectionTestClasses.privateStaticClassWithPrivateConstructor;
import static com.opower.common.reflect.hide.ReflectionTestClasses.privateStaticClassWithPublicConstructor;

/**
 * Tests the {@link Reflection} methods
 *
 * @author jeff@opower.com
 */
public class TestReflection {
    @Test
    public void testPublicStaticClassWithPublicConstructor() throws Exception {
        callCheckAndThrowCause(publicStaticClassWithPublicConstructor());
    }

    @Test(expected = IllegalAccessException.class)
    public void testPublicStaticClassWithPrivateConstructor() throws Exception {
        callCheckAndThrowCause(publicStaticClassWithPrivateConstructor());
    }

    @Test(expected = IllegalAccessException.class)
    public void testPublicStaticClassWithProtectedConstructor() throws Exception {
        callCheckAndThrowCause(publicStaticClassWithProtectedConstructor());
    }

    @Test(expected = InstantiationException.class)
    public void testPublicStaticClassWithNonDefaultConstructor() throws Exception {
        callCheckAndThrowCause(publicStaticClassWithNonDefaultConstructor());
    }

    @Test(expected = IllegalAccessException.class)
    public void testPrivateStaticClassWithPrivateConstructor() throws Exception {
        callCheckAndThrowCause(privateStaticClassWithPrivateConstructor());
    }

    @Test(expected = IllegalAccessException.class)
    public void testPrivateStaticClassWithPublicConstructor() throws Exception {
        callCheckAndThrowCause(privateStaticClassWithPublicConstructor());
    }

    private void callCheckAndThrowCause(Object object) throws Exception {
        try {
            Reflection.checkDeserializable(object);
        }
        catch (RuntimeException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception)cause;
            }
            throw e;
        }
    }
}
