package com.opower.hadoop.hbase.test;

import org.apache.hadoop.hbase.HBaseTestingUtility;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

/**
 * Tests the {@link HBaseTestRunner} by creating a test that is run with it and validating
 * that the appropriate behaviors have occurred, specifically with a static HBaseTestingUtility
 * and a {@code @BeforeClass} annotation
 *
 * @author jeff@opower.com
 */
@RunWith(HBaseTestRunner.class)
public class IntTestHBaseTestRunnerStatic {
    protected static HBaseTestingUtility hbaseTestingUtility;

    @BeforeClass
    public static void setUpClass() {
        assertNotNull(hbaseTestingUtility);
    }

    @Before
    public void setUp() {
        assertNotNull(hbaseTestingUtility);
    }

    @Test
    public void testThatMiniClusterIsRunning() {
    }
}
