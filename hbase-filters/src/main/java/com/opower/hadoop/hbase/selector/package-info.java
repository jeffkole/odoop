/**
 * An abstraction from HBase's native Filter that inverts the contract.
 * The intent of this abstraction is to make writing simple filters easier.
 * It is not intended to fully replace the Filter interface, which is clearly
 * designed for performance, given HBase's unique data access patterns.
 */
package com.opower.hadoop.hbase.selector;
