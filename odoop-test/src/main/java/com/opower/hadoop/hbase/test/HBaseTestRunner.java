package com.opower.hadoop.hbase.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.HBaseTestingUtility;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * A {@link Runner JUnit runner} that boots up a {@link MiniHBaseCluster} for the lifespan of all
 * tests that are run, thus allowing you to take the performance penalty of booting the cluster
 * once instead of with each test class that is run.  The flip side of this is that each test that
 * uses the mini cluster must definitely clean up after itself so that it does not affect any other
 * test that may run.
 * </p><p>
 * When a test class is run with this runner and has an {@link HBaseTestingUtility} member or static
 * field, that field will be populated with the {@link HBaseTestingUtility} that is shared for all
 * test classes in the test run.
 * </p><p>
 * This class is highly influenced by Spring's SpringJunit4ClassRunner and TestContextManager.
 *
 * @author jeff@opower.com
 */
public class HBaseTestRunner extends BlockJUnit4ClassRunner {
    private static final Log LOG = LogFactory.getLog(HBaseTestRunner.class);

    private static HBaseTestingUtility hBaseTestingUtility;

    private static synchronized HBaseTestingUtility initializeTestingUtility() throws Exception {
        if (hBaseTestingUtility == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Instantiating HBaseTestingUtility and booting mini cluster");
            }
            hBaseTestingUtility = new HBaseTestingUtility();
            hBaseTestingUtility.startMiniCluster();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Shutting down mini cluster");
                    }
                    try {
                        hBaseTestingUtility.shutdownMiniCluster();
                    }
                    catch (Exception e) {
                        LOG.warn("Exception shutting down mini cluster", e);
                    }
                }
            });
        }
        return hBaseTestingUtility;
    }

    public HBaseTestRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
    }

    /**
     * Prepares a test suite for running with a {@link MiniHBaseCluster}.  Instatiates an
     * {@link HBaseTestingUtility} if one is not already instantiated for the test suite
     * and populates any member field in the current test class with the resulting
     * {@link HBaseTestingUtility}.
     *
     * {@inheritDoc}
     */
    @Override
    protected Object createTest() throws Exception {
        Object testInstance = super.createTest();
        HBaseTestingUtility testingUtility = initializeTestingUtility();
        initializeTestInstance(testInstance, testingUtility);
        return testInstance;
    }

    /**
     * Prepares a test suite for running with a {@link MiniHBaseCluster}.  Instatiates an
     * {@link HBaseTestingUtility} if one is not already instantiated for the test suite
     * and populates any static field in the current test class with the resulting
     * {@link HBaseTestingUtility}.
     *
     * {@inheritDoc}
     */
    @Override
    protected Statement withBeforeClasses(Statement statement) {
        final Statement beforeClasses = super.withBeforeClasses(statement);
        final Class testClass = getTestClass().getJavaClass();
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                HBaseTestingUtility testingUtility = initializeTestingUtility();
                populateHBaseTestingUtility(null, testClass, testingUtility);
                beforeClasses.evaluate();
            }
        };
    }

    private void initializeTestInstance(Object test, HBaseTestingUtility testingUtility) throws Exception {
        populateHBaseTestingUtility(test, test.getClass(), testingUtility);
    }

    private void populateHBaseTestingUtility(Object test, Class clazz, HBaseTestingUtility testingUtility) throws Exception {
        if (clazz == null || clazz.equals(Object.class)) {
            return;
        }

        Field[] declaredFields = clazz.getDeclaredFields();
        for (Field field : declaredFields) {
            if (field.getType().equals(HBaseTestingUtility.class)) {
                field.setAccessible(true);
                if (Modifier.isStatic(field.getModifiers())) {
                    field.set(null, testingUtility);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Set static field %s in class %s", field, clazz));
                    }
                }
                else if (test != null) {
                    field.set(test, testingUtility);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Set field %s in %s", field, test));
                    }
                }
                return;
            }
        }
        // Try populating the field in a super-class
        populateHBaseTestingUtility(test, clazz.getSuperclass(), testingUtility);
    }
}
