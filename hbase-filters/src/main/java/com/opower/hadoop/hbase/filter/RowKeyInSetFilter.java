package com.opower.hadoop.hbase.filter;

import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.ByteBloomFilter;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An HBase filter that checks for the existence of a row key in a set, giving
 * functionality similar to that of an "in" clause in SQL.
 *
 * This implementation is based on a bloom filter, so more specifically it
 * filters out rows that are definitely not in the set.  You could consider it
 * a "not not in set" filter, in that regard, though thinking of it as a
 * filter that returns rows that are in the filter plus some that may not be
 * is probably a more sane way of conceptualizing it.
 *
 * @author jeff@opower.com
 */
public class RowKeyInSetFilter extends FilterBase {

    private ByteBloomFilter bloomFilter;
    private ByteBuffer bloomBits;

    /**
     * Default constructor needed for serialization; use
     * {@link RowKeyInSetFilter(ByteBloomFilter)} when you want to create
     * a RowKeyInSetFilter for real
     */
    public RowKeyInSetFilter() {}

    public RowKeyInSetFilter(ByteBloomFilter bloomFilter) throws IOException {
        this.bloomFilter = bloomFilter;
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteStream);
        this.bloomFilter.writeBloom(out);
        out.flush();
        out.close();
        this.bloomBits = ByteBuffer.wrap(byteStream.toByteArray());
    }

    /**
     * Filters our rows that are definitively not in the set.
     *
     * @inheritDoc
     */
    @Override
    public boolean filterRowKey(byte[] rowKeyBuffer, int offset, int length) {
        // If the row is found in the set, then return false to continue
        if (this.bloomFilter.contains(
                    rowKeyBuffer, offset, length, this.bloomBits)) {
            return false;
        }
        // Otherwise, we can skip the rest of processing for the row
        return true;
    }

    public void write(DataOutput out) throws IOException {
        // Write out the meta data that will be used to reconstruct the
        // bloom filter on the server
        this.bloomFilter.getMetaWriter().write(out);
        // Now write out the actual bloom filter bytes
        out.writeInt(this.bloomFilter.getByteSize());
        this.bloomFilter.getDataWriter().write(out);
    }

    public void readFields(DataInput in) throws IOException {
        int bytesPerInt = 4;
        int numIntsInMeta = 5;
        byte[] metaBytes = new byte[bytesPerInt * numIntsInMeta];
        in.readFully(metaBytes);
        int numBytesInData = in.readInt();
        byte[] rawBloom = new byte[numBytesInData];
        in.readFully(rawBloom);

        this.bloomFilter = new ByteBloomFilter(ByteBuffer.wrap(metaBytes));
        // allocate the buffer to make sure we can actually use it
        this.bloomFilter.allocBloom();
        this.bloomBits = ByteBuffer.wrap(rawBloom);
    }
}
