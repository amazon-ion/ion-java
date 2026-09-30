/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion;

import static com.amazon.ion.TestUtils.ensureBinary;
import static com.amazon.ion.TestUtils.ensureText;

import com.amazon.ion.impl._Private_IonConstants;
import com.amazon.ion.impl._Private_IonSystem;
import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.impl.bin.SystemSymbolResolvingIonRawBinaryWriter;
import com.amazon.ion.impl.bin._Private_IonManagedBinaryWriterBuilder;
import com.amazon.ion.impl.bin._Private_IonManagedWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;

/**
 * Abstracts the various ways that {@link IonReader}s can be created, so test
 * cases can cover all the APIs.
 */
public enum ReaderMaker
{
    /**
     * Invokes {@link IonSystem#newReader(String)}.
     */
    FROM_STRING(Feature.TEXT)
    {
        @Override
        public IonReader newReader(IonSystem system, String ionText)
        {
            return system.newReader(ionText);
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(byte[])} with Ion binary.
     */
    FROM_BYTES_BINARY(Feature.BINARY)
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureBinary(system, ionData);
            return system.newReader(ionData);
        }

        @Override
        public IonReader newReaderVerbatim(IonSystem system, String ionText) {
            return system.newReader(convertToBinaryVerbatim(system, ionText));
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(byte[])} with Ion text.
     */
    FROM_BYTES_TEXT(Feature.TEXT)
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureText(system, ionData);
            return system.newReader(ionData);
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(byte[],int,int)} with Ion binary.
     */
    FROM_BYTES_OFFSET_BINARY(Feature.BINARY)
    {
        @Override
        public int getOffset() { return 37; }

        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureBinary(system, ionData);
            byte[] padded = new byte[ionData.length + 70];
            System.arraycopy(ionData, 0, padded, 37, ionData.length);
            return system.newReader(padded, 37, ionData.length);
        }

        @Override
        public IonReader newReaderVerbatim(IonSystem system, String ionText) {
            byte[] ionData = convertToBinaryVerbatim(system, ionText);
            byte[] padded = new byte[ionData.length + 70];
            System.arraycopy(ionData, 0, padded, 37, ionData.length);
            return system.newReader(padded, 37, ionData.length);
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(byte[],int,int)} with Ion text.
     */
    FROM_BYTES_OFFSET_TEXT(Feature.TEXT)
    {
        @Override
        public int getOffset() { return 37; }

        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureText(system, ionData);
            byte[] padded = new byte[ionData.length + 70];
            System.arraycopy(ionData, 0, padded, 37, ionData.length);
            return system.newReader(padded, 37, ionData.length);
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(java.nio.ByteBuffer)} with an
     * array-backed {@link ByteBuffer} over Ion binary.
     */
    FROM_BYTE_BUFFER_BINARY(Feature.BINARY)
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureBinary(system, ionData);
            return system.newReader(ByteBuffer.wrap(ionData));
        }

        @Override
        public IonReader newReaderVerbatim(IonSystem system, String ionText) {
            return system.newReader(ByteBuffer.wrap(convertToBinaryVerbatim(system, ionText)));
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(java.nio.ByteBuffer)} with an
     * array-backed {@link ByteBuffer} over Ion text.
     */
    FROM_BYTE_BUFFER_TEXT(Feature.TEXT)
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureText(system, ionData);
            return system.newReader(ByteBuffer.wrap(ionData));
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(java.nio.ByteBuffer)} with a direct
     * (off-heap) {@link ByteBuffer} over Ion binary, exercising the non-array
     * code path.
     */
    FROM_BYTE_BUFFER_DIRECT_BINARY(Feature.BINARY)
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureBinary(system, ionData);
            return system.newReader(directBuffer(ionData));
        }

        @Override
        public IonReader newReaderVerbatim(IonSystem system, String ionText) {
            return system.newReader(directBuffer(convertToBinaryVerbatim(system, ionText)));
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(java.nio.ByteBuffer)} with a read-only
     * {@link ByteBuffer} over Ion binary, exercising the read-only code path.
     */
    FROM_BYTE_BUFFER_READ_ONLY_BINARY(Feature.BINARY)
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureBinary(system, ionData);
            return system.newReader(ByteBuffer.wrap(ionData).asReadOnlyBuffer());
        }

        @Override
        public IonReader newReaderVerbatim(IonSystem system, String ionText) {
            return system.newReader(ByteBuffer.wrap(convertToBinaryVerbatim(system, ionText)).asReadOnlyBuffer());
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(java.nio.ByteBuffer)} with a sliced
     * {@link ByteBuffer} whose readable region is a sub-range of a larger
     * backing array (non-zero {@code position} and {@code arrayOffset}),
     * over Ion binary. Note: the resulting reader is created over a zero-based
     * copy of the sub-range, so its octet offsets are stable at 0 (like
     * {@link #FROM_BYTE_BUFFER_BINARY}); this maker verifies that sub-range
     * extraction reads exactly the intended bytes.
     */
    FROM_BYTE_BUFFER_OFFSET_BINARY(Feature.BINARY)
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureBinary(system, ionData);
            return system.newReader(slicedBuffer(ionData, 37, 70));
        }

        @Override
        public IonReader newReaderVerbatim(IonSystem system, String ionText) {
            return system.newReader(slicedBuffer(convertToBinaryVerbatim(system, ionText), 37, 70));
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(java.nio.ByteBuffer)} with an
     * array-backed {@link ByteBuffer} over Ion text (via a sliced sub-range).
     */
    FROM_BYTE_BUFFER_OFFSET_TEXT(Feature.TEXT)
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureText(system, ionData);
            return system.newReader(slicedBuffer(ionData, 37, 70));
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(InputStream)} with Ion binary.
     */
    FROM_INPUT_STREAM_BINARY(Feature.BINARY, Feature.STREAM)
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData,
                                   InputStreamWrapper wrapper)
            throws IOException
        {
            ionData = ensureBinary(system, ionData);
            InputStream in = new ByteArrayInputStream(ionData);
            InputStream wrapped = wrapper.wrap(in);
            return system.newReader(wrapped);
        }

        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureBinary(system, ionData);
            InputStream in = new ByteArrayInputStream(ionData);
            return system.newReader(in);
        }

        @Override
        public IonReader newReaderVerbatim(IonSystem system, String ionText) {
            return system.newReader(new ByteArrayInputStream(convertToBinaryVerbatim(system, ionText)));
        }
    },

    /**
     * Invokes {@link IonSystem#newReader(InputStream)} with Ion text.
     */
    FROM_INPUT_STREAM_TEXT(Feature.TEXT, Feature.STREAM)
    {
        @Override
        public IonReader newReader(IonSystem system, byte[] ionData,
                                   InputStreamWrapper wrapper)
            throws IOException
        {
            ionData = ensureText(system, ionData);
            InputStream in = new ByteArrayInputStream(ionData);
            InputStream wrapped = wrapper.wrap(in);
            return system.newReader(wrapped);
        }

        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            ionData = ensureText(system, ionData);
            InputStream in = new ByteArrayInputStream(ionData);
            return system.newReader(in);
        }
    },


    /**
     * Invokes {@link IonSystem#newReader(Reader)}.
     */
    FROM_READER(Feature.TEXT)
    {
        @Override
        public IonReader newReader(IonSystem system, String ionText)
        {
            Reader reader = new StringReader(ionText);
            return system.newReader(reader);
        }
    },


    FROM_DOM(Feature.DOM)
    {
        @Override
        public IonReader newReader(IonSystem system, String ionText)
        {
            IonDatagram dg = system.getLoader().load(ionText);
            return system.newReader(dg);
        }

        @Override
        public IonReader newReader(IonSystem system, byte[] ionData)
        {
            IonDatagram dg = system.getLoader().load(ionData);
            return system.newReader(dg);
        }

        @Override
        public IonReader newReaderVerbatim(IonSystem system, String ionText) {
            Iterator<IonValue> systemIterator = ((_Private_IonSystem) system).systemIterate(
                ((_Private_IonSystem) system).newSystemReader(ionText)
            );
            IonDatagram dg = system.newDatagram();
            while (systemIterator.hasNext()) {
                dg.add(systemIterator.next());
            }
            return system.newReader(dg);
        }
    };


    //========================================================================

    public enum Feature { TEXT, BINARY, DOM, STREAM }

    private final EnumSet<Feature> myFeatures;


    private ReaderMaker(Feature feature1, Feature... features)
    {
        myFeatures = EnumSet.of(feature1, features);
    }


    public boolean sourceIsText()
    {
        return myFeatures.contains(Feature.TEXT);
    }

    public boolean sourceIsBinary()
    {
        return myFeatures.contains(Feature.BINARY);
    }


    public int getOffset()
    {
        return 0;
    }


    public IonReader newReader(IonSystem system, String ionText)
    {
        byte[] utf8 = _Private_Utils.utf8(ionText);
        return newReader(system, utf8);
    }

    /**
     * Create a reader over an Ion stream that is identical to the given Ion text, even at the system level. In
     * other words, symbol table boundaries and symbol ID assignments are preserved. Any symbol tables in the provided
     * text are not parsed as system values, so open content is preserved. Note: certain aspects of the text encoding
     * have no binary Ion 1.0 equivalent, such as symbol tokens not present in any symbol table. If such text is
     * provided to this method, the output may not be valid when converted to binary.
     * @param system an IonSystem instance.
     * @param ionText the text Ion to read verbatim.
     * @return a new IonReader.
     */
    public IonReader newReaderVerbatim(IonSystem system, String ionText) {
        // Note: this is the default implementation when the requested source type is text Ion; no conversion is
        // necessary. Binary Ion and DOM sources override this method and perform a verbatim conversion.
        return newReader(system, ionText);
    }

    /**
     * Converts the given text Ion to a verbatim representation in binary.
     * @param system an IonSystem instance.
     * @param ionText the text Ion to convert verbatim.
     * @return the converted binary Ion.
     */
    private static byte[] convertToBinaryVerbatim(IonSystem system, String ionText) {
        IonReader reader = ((_Private_IonSystem) system).newSystemReader(ionText);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (IonWriter rawWriter = new SystemSymbolResolvingIonRawBinaryWriter(
            _Private_IonManagedBinaryWriterBuilder
                .create(_Private_IonManagedBinaryWriterBuilder.AllocatorMode.POOLED)
                .newWriter(out)
                .asFacet(_Private_IonManagedWriter.class)
                .getRawWriter()
        )) {
            out.write(_Private_IonConstants.BINARY_VERSION_MARKER_1_0);
            rawWriter.writeValues(reader);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return out.toByteArray();
    }

    /**
     * Copies the given bytes into a direct (off-heap) {@link ByteBuffer} whose
     * readable region is exactly {@code data}.
     */
    private static ByteBuffer directBuffer(byte[] data) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(data.length);
        buffer.put(data);
        buffer.flip();
        return buffer;
    }

    /**
     * Places the given bytes into a larger array-backed buffer at
     * {@code offset}, then returns a {@link ByteBuffer#slice()} whose readable
     * region is exactly {@code data}. The slice has a non-zero
     * {@code arrayOffset}, exercising sub-range extraction.
     *
     * @param data the payload bytes.
     * @param offset the offset within the padded backing array at which the
     * payload is placed.
     * @param extraPadding total extra capacity (split before/after the payload)
     * of the backing array.
     */
    private static ByteBuffer slicedBuffer(byte[] data, int offset, int extraPadding) {
        byte[] padded = new byte[data.length + extraPadding];
        System.arraycopy(data, 0, padded, offset, data.length);
        ByteBuffer backing = ByteBuffer.wrap(padded);
        backing.position(offset);
        backing.limit(offset + data.length);
        // slice() yields a buffer whose position is 0, limit/capacity are the
        // payload length, and arrayOffset() reflects the sub-range start.
        return backing.slice();
    }

    public IonReader newReader(IonSystem system, byte[] ionData)
    {
        IonDatagram dg = system.getLoader().load(ionData);
        String ionText = dg.toString();
        return newReader(system, ionText);
    }


    public IonReader newReader(IonSystem system, byte[] ionData,
                               InputStreamWrapper wrapper)
        throws IOException
    {
        return newReader(system, ionData);
    }


    public IonReader newReader(IonSystem system, InputStream ionData)
        throws IOException
    {
        byte[] bytes = _Private_Utils.loadStreamBytes(ionData);
        return newReader(system, bytes);
    }


    public static ReaderMaker[] valuesExcluding(ReaderMaker... exclusions)
    {
        ReaderMaker[] all = values();
        ArrayList<ReaderMaker> retained =
            new ArrayList<ReaderMaker>(Arrays.asList(all));
        retained.removeAll(Arrays.asList(exclusions));
        return retained.toArray(new ReaderMaker[retained.size()]);
    }

    public static ReaderMaker[] valuesWith(Feature feature)
    {
        ReaderMaker[] all = values();
        ArrayList<ReaderMaker> retained = new ArrayList<ReaderMaker>();
        for (ReaderMaker maker : all)
        {
            if (maker.myFeatures.contains(feature))
            {
                retained.add(maker);
            }
        }
        return retained.toArray(new ReaderMaker[retained.size()]);
    }

    public static ReaderMaker[] valuesWithout(Feature feature)
    {
        ReaderMaker[] all = values();
        ArrayList<ReaderMaker> retained = new ArrayList<ReaderMaker>();
        for (ReaderMaker maker : all)
        {
            if (! maker.myFeatures.contains(feature))
            {
                retained.add(maker);
            }
        }
        return retained.toArray(new ReaderMaker[retained.size()]);
    }
}
