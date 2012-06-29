package com.opower.hadoop.hbase.selector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.hbase.util.ByteBloomFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;

/**
 * A {@link Selector} that checks for the existence of a row key in a set, giving functionality similar to that of
 * an "in" clause in SQL.
 *
 * This implementation is based on a bloom filter, so more specifically it filters out rows that are definitely
 * not in the set.  You could consider it a "not not in set" filter, in that regard, though thinking of it as a
 * filter that returns rows that are in the filter plus some that may not be is probably a more sane way of
 * conceptualizing it.
 *
 * @author jeff@opower.com
 */
public class RowKeyInSetSelector extends AbstractRowSelector {
    private static final Log LOG = LogFactory.getLog(RowKeyInSetSelector.class);

    private ByteBloomFilter bloomFilter;
    private ByteBuffer bloomBits;

    /**
     * Default constructor needed for serialization; use {@link #RowKeyInSetSelector(ByteBloomFilter)}
     * when you want to create one for real
     */
    public RowKeyInSetSelector() {}

    public RowKeyInSetSelector(ByteBloomFilter bloomFilter) {
        this.bloomFilter = bloomFilter;
        // We do not need to populate the bloomBits field, because it is only used after the filter has been
        // serialized, sent to the server, and deserialized.  But, to make testing easier, we will do it now, too.
        this.bloomBits = getBloomBits(this.bloomFilter);
        outputStats(this.bloomFilter);
    }

    public RowKeyInSetSelector(Collection<String> rowKeys) {
        int size = rowKeys.size();
        this.bloomFilter = new ByteBloomFilter(size, 0.0001f, Hash.JENKINS_HASH, 10);
        this.bloomFilter.allocBloom();
        for (String rowKey : rowKeys) {
            this.bloomFilter.add(Bytes.toBytes(rowKey));
        }
        this.bloomBits = getBloomBits(this.bloomFilter);
        outputStats(this.bloomFilter);
    }

    private ByteBuffer getBloomBits(ByteBloomFilter bloomFilter) {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(byteStream);
            bloomFilter.getDataWriter().write(out);
            out.flush();
            out.close();
            return ByteBuffer.wrap(byteStream.toByteArray());
        }
        catch (IOException ioe) {
            // There really is not much that can be done about this, so just rethrow as runtime
            throw new RuntimeException(ioe);
        }
    }

    /**
     * Only includes rows that are very likely in the set, due to the nature of a Bloom filter
     *
     * {@inheritDoc}
     */
    @Override
    public boolean includeRow(byte[] buffer, int offset, int length) {
        // If the row is found in the set, then return false to continue
        return (this.bloomFilter.contains(buffer, offset, length, this.bloomBits));
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

        outputStats(this.bloomFilter);
    }

    private static void outputStats(ByteBloomFilter filter) {
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("%s: %d; %s: %d; %s: %d",
                        "Byte size", filter.getByteSize(),
                        "Key count", filter.getKeyCount(),
                        "Max keys", filter.getMaxKeys()));
        }
    }
}