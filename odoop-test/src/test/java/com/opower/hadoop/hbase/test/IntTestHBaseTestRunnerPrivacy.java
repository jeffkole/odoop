package com.opower.hadoop.hbase.test;

import org.apache.hadoop.hbase.HBaseTestingUtility;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

/**
 * Tests the {@link HBaseTestRunner} by creating a test that is run with it and validating
 * that the appropriate behaviors have occurred, specifically focused on populating private
 * fields.
 *
 * @author jeff@opower.com
 */
@RunWith(HBaseTestRunner.class)
public class IntTestHBaseTestRunnerPrivacy {
    private HBaseTestingUtility hbaseTestingUtility;

    @Before
    public void setUp() {
        assertNotNull(this.hbaseTestingUtility);
    }

    @Test
    public void testThatMiniClusterIsRunning() {
    }
}
