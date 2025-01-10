// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTextReader;
import com.amazon.ion.IonValue;
import com.amazon.ion.util.InputStreamInterceptor;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.util.IonStreamUtils;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Collections;
import java.util.List;

import static com.amazon.ion.impl.LocalSymbolTable.DEFAULT_LST_FACTORY;
import static com.amazon.ion.impl._Private_IonReaderFactory.makeReader;
import static com.amazon.ion.impl._Private_IonReaderFactory.makeReaderText;

/**
 * {@link IonReaderBuilder} extension for internal use only.
 */
public class _Private_IonReaderBuilder extends IonReaderBuilder {

    private _Private_LocalSymbolTableFactory lstFactory;

    private _Private_IonReaderBuilder() {
        super();
        lstFactory = DEFAULT_LST_FACTORY;
    }

    private _Private_IonReaderBuilder(_Private_IonReaderBuilder that) {
        super(that);
        this.lstFactory = that.lstFactory;
    }

    /**
     * Declares the {@link _Private_LocalSymbolTableFactory} to use when constructing applicable readers.
     *
     * @param factory the factory to use, or {@link LocalSymbolTable#DEFAULT_LST_FACTORY} if null.
     *
     * @return this builder instance, if mutable;
     * otherwise a mutable copy of this builder.
     *
     * @see #setLstFactory(_Private_LocalSymbolTableFactory)
     */
    public IonReaderBuilder withLstFactory(_Private_LocalSymbolTableFactory factory) {
        _Private_IonReaderBuilder b = (_Private_IonReaderBuilder) mutable();
        b.setLstFactory(factory);
        return b;
    }

    /**
     * @see #withLstFactory(_Private_LocalSymbolTableFactory)
     */
    public void setLstFactory(_Private_LocalSymbolTableFactory factory) {
        mutationCheck();
        if (factory == null) {
            lstFactory = DEFAULT_LST_FACTORY;
        } else {
            lstFactory = factory;
        }
    }

    public static class Mutable extends _Private_IonReaderBuilder {

        public Mutable() {
        }

        public Mutable(IonReaderBuilder that) {
            super((_Private_IonReaderBuilder) that);
        }

        @Override
        public IonReaderBuilder immutable() {
            return new _Private_IonReaderBuilder(this);
        }

        @Override
        public IonReaderBuilder mutable() {
            return this;
        }

        @Override
        protected void mutationCheck() {
        }

    }

    /**
     * InputStream that reads from exactly two delegate InputStreams in sequence. The second delegate remains
     * in place even if it indicates EOF during a read call. This behavior differentiates this implementation from
     * SequenceInputStream, which will return EOF forever after its final delegate returns EOF for the first time.
     * The TwoElementSequenceInputStream allows the second delegate InputStream to return valid data if it subsequently
     * receives more data, which is common when performing continuable reads.
     */
    private static final class TwoElementInputStream extends InputStream {

        /**
         * The first InputStream in the sequence.
         */
        private final InputStream first;

        /**
         * The last InputStream in the sequence.
         */
        private final InputStream last;

        /**
         * The current InputStream.
         */
        private InputStream in;

        /**
         * Constructor.
         * @param first first InputStream in the sequence.
         * @param last last InputStream in the sequence.
         */
        private TwoElementInputStream(final InputStream first, final InputStream last) {
            this.first = first;
            this.last = last;
            this.in = first;
        }

        @Override
        public int available() throws IOException {
            return first.available() + last.available();
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b < 0 && in == first) {
                in = last;
                b = in.read();
            }
            return b;
        }

        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {
            int bytesToRead = len;
            int bytesRead = 0;
            int destinationOffset = off;
            while (true) {
                int bytesReadThisIteration = in.read(b, destinationOffset, bytesToRead);
                if (bytesReadThisIteration < 0) {
                    if (in == first) {
                        in = last;
                        continue;
                    }
                    break;
                }
                bytesRead += bytesReadThisIteration;
                if (bytesRead == len) {
                    break;
                } else if (in == last) {
                    // There's no other source of bytes, so return fewer bytes than requested.
                    break;
                }
                bytesToRead -= bytesReadThisIteration;
                destinationOffset += bytesReadThisIteration;
            }
            if (bytesRead > 0) {
                return bytesRead;
            }
            return -1;
        }

        @Override
        public void close() throws IOException {
            try {
                first.close();
            } finally {
                last.close();
            }
        }
    }

    @FunctionalInterface
    interface IonReaderFromBytesFactoryText {
        IonReader makeReader(IonCatalog catalog, byte[] ionData, int offset, int length, _Private_LocalSymbolTableFactory lstFactory);
    }

    @FunctionalInterface
    interface IonReaderFromBytesFactoryBinary {
        IonReader makeReader(_Private_IonReaderBuilder builder, byte[] ionData, int offset, int length);
    }

    private static void validateHeaderLength(int maxHeaderLength) {
        if (maxHeaderLength > _Private_IonConstants.ARRAY_MAXIMUM_SIZE) {
            // Note: we could choose an arbitrary limit lower than this. The purpose at this point is to avoid OOM
            // in the case where Java cannot allocate an array of the requested size.
            throw new IonException(String.format(
                "The maximum header length %d exceeds the maximum array size %d.",
                maxHeaderLength,
                _Private_IonConstants.ARRAY_MAXIMUM_SIZE
            ));
        }
    }

    static IonReader buildReader(
        _Private_IonReaderBuilder builder,
        byte[] ionData,
        int offset,
        int length,
        IonReaderFromBytesFactoryBinary binary,
        IonReaderFromBytesFactoryText text
    ) {
        List<InputStreamInterceptor> streamInterceptors = builder.getInputStreamInterceptors();
        for (InputStreamInterceptor streamInterceptor : streamInterceptors) {
            int headerLength = streamInterceptor.headerMatchLength();
            validateHeaderLength(headerLength);
            if (length < headerLength) {
                continue;
            }
            if (streamInterceptor.matchesHeader(ionData, offset, length)) {
                try {
                    return buildReader(
                        builder,
                        streamInterceptor.newInputStream(new ByteArrayInputStream(ionData, offset, length)),
                        _Private_IonReaderFactory::makeReaderBinary,
                        _Private_IonReaderFactory::makeReaderText,
                        // The builder provides only one level of detection, e.g. GZIP-compressed binary Ion *or*
                        // zstd-compressed binary Ion; *not* GZIP-compressed zstd-compressed binary Ion. Users that
                        // need to intercept multiple format layers can provide a custom InputStreamInterceptor to
                        // achieve this.
                        /*inputStreamInterceptors=*/ Collections.emptyList()
                    );
                } catch (IOException e) {
                    throw new IonException(e);
                }
            }
        }
        if (IonStreamUtils.isIonBinary(ionData, offset, length)) {
            return binary.makeReader(builder, ionData, offset, length);
        }
        return text.makeReader(builder.validateCatalog(), ionData, offset, length, builder.lstFactory);
    }

    @Override
    public IonReader build(byte[] ionData, int offset, int length)
    {
        return buildReader(
            this,
            ionData,
            offset,
            length,
            _Private_IonReaderFactory::makeReaderBinary,
            _Private_IonReaderFactory::makeReaderText
        );
    }

    /**
     * Determines whether a stream that begins with the bytes in the provided buffer could be binary Ion.
     * @param buffer up to the first four bytes in a stream.
     * @param length the actual number of bytes in the buffer.
     * @return true if the first 'length' bytes in 'buffer' match the first 'length' bytes in the binary IVM.
     */
    private static boolean startsWithIvm(byte[] buffer, int length) {
        if (length >= _Private_IonConstants.BINARY_VERSION_MARKER_SIZE) {
            return buffer[0] == (byte) 0xE0
                && buffer[3] == (byte) 0xEA;
        } else if (length >= 1) {
            return buffer[0] == (byte) 0xE0;
        }
        return true;
    }

    @FunctionalInterface
    interface IonReaderFromInputStreamFactoryText {
        IonReader makeReader(IonCatalog catalog, InputStream source, _Private_LocalSymbolTableFactory lstFactory);
    }

    @FunctionalInterface
    interface IonReaderFromInputStreamFactoryBinary {
        IonReader makeReader(_Private_IonReaderBuilder builder, InputStream source, byte[] alreadyRead, int alreadyReadOff, int alreadyReadLen);
    }

    /**
     * Reads from the given source into the given byte array, stopping once either
     * <ol>
     *     <li>`length` bytes have been read, or</li>
     *     <li>the end of the source stream has been reached, or</li>
     *     <li>the source stream throws an exception.</li>
     * </ol>
     * @param source the source of the bytes to read.
     * @param destination the destination for the bytes read.
     * @param length the number of bytes to attempt to read.
     * @return the number of bytes read into `destination`.
     */
    private static int fillToLengthOrStreamEnd(InputStream source, byte[] destination, int length) {
        int bytesRead = 0;
        while (bytesRead < length) {
            int bytesToRead = length - bytesRead;
            int bytesReadThisIteration;
            try {
                bytesReadThisIteration = source.read(destination, bytesRead, bytesToRead);
            } catch (EOFException e) {
                // Some InputStream implementations throw EOFException in certain cases to convey
                // that the end of the stream has been reached.
                break;
            } catch (IOException e) {
                throw new IonException(e);
            }
            if (bytesReadThisIteration < 0) { // This indicates the end of the stream.
                break;
            }
            bytesRead += bytesReadThisIteration;
        }
        return bytesRead;
    }

    static IonReader buildReader(
        _Private_IonReaderBuilder builder,
        InputStream source,
        IonReaderFromInputStreamFactoryBinary binary,
        IonReaderFromInputStreamFactoryText text,
        List<InputStreamInterceptor> inputStreamInterceptors
    ) {
        if (source == null) {
            throw new NullPointerException("Cannot build a reader from a null InputStream.");
        }
        int maxHeaderLength = Math.max(
            _Private_IonConstants.BINARY_VERSION_MARKER_SIZE,
            inputStreamInterceptors.stream().mapToInt(InputStreamInterceptor::headerMatchLength).max().orElse(0)
        );
        validateHeaderLength(maxHeaderLength);
        // Note: this can create a lot of layers of InputStream wrappers. For example, if this method is called
        // from build(byte[]) and the bytes contain GZIP, the chain will be SequenceInputStream(ByteArrayInputStream,
        // GZIPInputStream -> PushbackInputStream -> ByteArrayInputStream). If this creates a drag on efficiency,
        // alternatives should be evaluated.
        byte[] possibleIVM = new byte[maxHeaderLength];
        InputStream ionData = source;
        int bytesRead = fillToLengthOrStreamEnd(ionData, possibleIVM, maxHeaderLength);
        // If the input stream is growing, it is possible that fewer than BINARY_VERSION_MARKER_SIZE bytes are
        // available yet. Simply check whether the stream *could* contain binary Ion based on the available bytes.
        // If it can't, fall back to text.
        // NOTE: if incremental text reading is added, there will need to be logic that handles the case where
        // the reader is created with 0 bytes available, as it is impossible to determine text vs. binary without
        // reading at least one byte. Currently, in that case, just create a binary incremental reader. Either the
        // stream will always be empty (in which case it doesn't matter whether a text or binary reader is used)
        // or it's a binary stream (in which case the correct reader was created) or it's a growing text stream
        // (which has always been unsupported).
        for (InputStreamInterceptor streamInterceptor : inputStreamInterceptors) {
            if (bytesRead < streamInterceptor.headerMatchLength()) {
                continue;
            }
            if (streamInterceptor.matchesHeader(possibleIVM, 0, bytesRead)) {
                try {
                    ionData = streamInterceptor.newInputStream(
                        new TwoElementInputStream(new ByteArrayInputStream(possibleIVM, 0, bytesRead), ionData)
                    );
                } catch (IOException e) {
                    throw new IonException(e);
                }
                bytesRead = fillToLengthOrStreamEnd(ionData, possibleIVM, _Private_IonConstants.BINARY_VERSION_MARKER_SIZE);
                break;
            }
        }
        if (startsWithIvm(possibleIVM, bytesRead)) {
            return binary.makeReader(builder, ionData, possibleIVM, 0, bytesRead);
        }
        InputStream wrapper;
        if (bytesRead > 0) {
            wrapper = new TwoElementInputStream(
                new ByteArrayInputStream(possibleIVM, 0, bytesRead),
                ionData
            );
        } else {
            wrapper = ionData;
        }
        return text.makeReader(builder.validateCatalog(), wrapper, builder.lstFactory);
    }

    @Override
    public IonReader build(InputStream source)
    {
        return buildReader(
            this,
            source,
            _Private_IonReaderFactory::makeReaderBinary,
            _Private_IonReaderFactory::makeReaderText,
            getInputStreamInterceptors()
        );
    }

    @Override
    public IonReader build(Reader ionText) {
        return makeReaderText(validateCatalog(), ionText, lstFactory);
    }

    @Override
    public IonReader build(IonValue value) {
        return makeReader(validateCatalog(), value, lstFactory);
    }

    @Override
    public IonTextReader build(String ionText) {
        return makeReaderText(validateCatalog(), ionText, lstFactory);
    }

}
