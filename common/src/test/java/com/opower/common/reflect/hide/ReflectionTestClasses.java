package com.opower.common.reflect.hide;

/**
 * A holder class of classes with different visibility levels to properly test the
 * {@link Reflection} methods
 *
 * @author jeff@opower.com
 */
public final class ReflectionTestClasses {
    private ReflectionTestClasses() {}

    /**
     * A class declaration merely for testing
     */
    public static class PublicStaticClassWithPublicConstructor {
        public PublicStaticClassWithPublicConstructor() {
        }
    }

    public static Object publicStaticClassWithPublicConstructor() {
        return new PublicStaticClassWithPublicConstructor();
    }

    /**
     * A class declaration merely for testing
     */
    public static final class PublicStaticClassWithPrivateConstructor {
        private PublicStaticClassWithPrivateConstructor() {
        }
    }

    public static Object publicStaticClassWithPrivateConstructor() {
        return new PublicStaticClassWithPrivateConstructor();
    }

    /**
     * A class declaration merely for testing
     */
    public static class PublicStaticClassWithProtectedConstructor {
        protected PublicStaticClassWithProtectedConstructor() {
        }
    }

    public static Object publicStaticClassWithProtectedConstructor() {
        return new PublicStaticClassWithProtectedConstructor();
    }

    /**
     * A class declaration merely for testing
     */
    public static class PublicStaticClassWithNonDefaultConstructor {
        public PublicStaticClassWithNonDefaultConstructor(int number) {
        }
    }

    public static Object publicStaticClassWithNonDefaultConstructor() {
        return new PublicStaticClassWithNonDefaultConstructor(42);
    }

    /**
     * A class declaration merely for testing
     */
    private static final class PrivateStaticClassWithPrivateConstructor {
        private PrivateStaticClassWithPrivateConstructor() {
        }
    }

    public static Object privateStaticClassWithPrivateConstructor() {
        return new PrivateStaticClassWithPrivateConstructor();
    }

    /**
     * A class declaration merely for testing
     */
    private static class PrivateStaticClassWithPublicConstructor {
        public PrivateStaticClassWithPublicConstructor() {
        }
    }

    public static Object privateStaticClassWithPublicConstructor() {
        return new PrivateStaticClassWithPublicConstructor();
    }
}
