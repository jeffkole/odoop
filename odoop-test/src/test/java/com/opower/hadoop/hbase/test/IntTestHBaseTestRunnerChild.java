package com.opower.hadoop.hbase.test;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests the {@link HBaseTestRunner} by creating a test that is run with it and validating
 * that the appropriate behaviors have occurred, specifically that class hierarchies work
 * well with the {@link HBaseTestRunner}.
 *
 * @author jeff@opower.com
 */
public class IntTestHBaseTestRunnerChild extends IntTestHBaseTestRunner {
    @Before
    public void setUp() {
        assertNotNull(this.hbaseTestingUtility);
    }

    @Test
    public void testThatMiniClusterIsRunning() {
    }
}
