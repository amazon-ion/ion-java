package com.amazon.ion.impl;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.Timestamp;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.util.RepeatInputStream;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.math.BigDecimal;

import static com.amazon.ion.impl._Private_IonConstants.BINARY_VERSION_MARKER_1_0;
import static org.junit.Assert.assertEquals;

public class IonReaderBinaryRawLargeStreamTest {

    // NOTE: this test takes several seconds to complete.
    @Test
    public void testReadLargeScalarStream() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IonWriter writer = IonBinaryWriterBuilder.standard().build(out);
        final Timestamp timestamp = Timestamp.forDay(2000, 1, 1);
        writer.writeString("foo");
        writer.writeDecimal(BigDecimal.TEN);
        writer.writeTimestamp(timestamp);
        writer.close();
        byte[] dataWithIvm = out.toByteArray();
        // Strip the IVM, as this needs to be one continuous stream to avoid resetting the reader's internals.
        byte[] data = new byte[dataWithIvm.length - BINARY_VERSION_MARKER_1_0.length];
        System.arraycopy(dataWithIvm, BINARY_VERSION_MARKER_1_0.length, data, 0, data.length);
        // The binary reader uses Integer.MIN_VALUE to mean NO_LIMIT for its _local_remaining value, which keeps track
        // of the remaining number of bytes in the current value. Between values at the top level, this should always be
        // NO_LIMIT. No arithmetic should ever be performed on the value when it is set to NO_LIMIT. If bugs exist that
        // violate this, then between top level values _local_remaining will never again be NO_LIMIT, meaning that
        // arithmetic will continue to be performed on it. Eventually, due to integer overflow, the value will roll over
        // into a small enough positive value that the reader will erroneously determine that there are fewer bytes
        // remaining than are needed to complete the current value. The reader will then finish early before reading the
        // entire stream. The bug that prompted this test to be written involved an unconditional subtraction of the
        // current value's length as declared in its header from the current value of _local_remaining within
        // stringValue(), decimalValue(), and timestampValue(). This caused _local_remaining to overflow to a very
        // large value immediately. For every top level value subsequently read, the length of that value would be
        // subtracted from _local_remaining until eventually _local_remaining prematurely reached 0 around the time
        // the stream reached Integer.MAX_VALUE in length.
        // Repeat the batch a sufficient number of times to exceed a total stream length of Integer.MAX_VALUE, plus
        // a few more to make sure batches continue to be read correctly.
        final int totalNumberOfBatches = (Integer.MAX_VALUE / data.length) + 7;
        InputStream inputStream = new SequenceInputStream(
            new ByteArrayInputStream(BINARY_VERSION_MARKER_1_0),
            new RepeatInputStream(data, totalNumberOfBatches - 1) // This will provide the data 'totalNumberOfBatches' times
        );
        IonReader reader = IonReaderBuilder.standard().build(inputStream);
        reader.next();
        assertEquals("foo", reader.stringValue());
        reader.next();
        assertEquals(BigDecimal.TEN, reader.decimalValue());
        reader.next();
        assertEquals(timestamp, reader.timestampValue());
        int batchesRead = 1;
        while (reader.next() != null) {
            assertEquals(IonType.STRING, reader.getType());
            assertEquals(IonType.DECIMAL, reader.next());
            assertEquals(IonType.TIMESTAMP, reader.next());
            batchesRead++;
        }
        assertEquals(totalNumberOfBatches, batchesRead);
    }
}
