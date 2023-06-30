// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTextReader;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.util.IonStreamUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.util.zip.GZIPInputStream;

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

    @FunctionalInterface
    interface IonReaderFromBytesFactoryText {
        IonReader makeReader(IonCatalog catalog, byte[] ionData, int offset, int length, _Private_LocalSymbolTableFactory lstFactory);
    }

    @FunctionalInterface
    interface IonReaderFromBytesFactoryBinary {
        IonReader makeReader(_Private_IonReaderBuilder builder, byte[] ionData, int offset, int length);
    }

    static IonReader buildReader(
        _Private_IonReaderBuilder builder,
        byte[] ionData,
        int offset,
        int length,
        IonReaderFromBytesFactoryBinary binary,
        IonReaderFromBytesFactoryText text
    ) {
        if (IonStreamUtils.isGzip(ionData, offset, length)) {
            try {
                return buildReader(
                    builder,
                    new GZIPInputStream(new ByteArrayInputStream(ionData, offset, length)),
                    _Private_IonReaderFactory::makeReaderBinary,
                    _Private_IonReaderFactory::makeReaderText
                );
            } catch (IOException e) {
                throw new IonException(e);
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

    static final byte[] GZIP_HEADER = {0x1F, (byte) 0x8B};

    private static boolean startsWithGzipHeader(byte[] buffer, int length) {
        if (length >= GZIP_HEADER.length) {
            return buffer[0] == GZIP_HEADER[0] && buffer[1] == GZIP_HEADER[1];
        }
        return false;
    }

    @FunctionalInterface
    interface IonReaderFromInputStreamFactoryText {
        IonReader makeReader(IonCatalog catalog, InputStream source, _Private_LocalSymbolTableFactory lstFactory);
    }

    @FunctionalInterface
    interface IonReaderFromInputStreamFactoryBinary {
        IonReader makeReader(_Private_IonReaderBuilder builder, InputStream source, byte[] alreadyRead, int alreadyReadOff, int alreadyReadLen);
    }

    static IonReader buildReader(
        _Private_IonReaderBuilder builder,
        InputStream source,
        IonReaderFromInputStreamFactoryBinary binary,
        IonReaderFromInputStreamFactoryText text
    ) {
        if (source == null) {
            throw new NullPointerException("Cannot build a reader from a null InputStream.");
        }
        // Note: this can create a lot of layers of InputStream wrappers. For example, if this method is called
        // from build(byte[]) and the bytes contain GZIP, the chain will be SequenceInputStream(ByteArrayInputStream,
        // GZIPInputStream -> PushbackInputStream -> ByteArrayInputStream). If this creates a drag on efficiency,
        // alternatives should be evaluated.
        byte[] possibleIVM = new byte[_Private_IonConstants.BINARY_VERSION_MARKER_SIZE];
        InputStream ionData = source;
        int bytesRead;
        try {
            bytesRead = ionData.read(possibleIVM);
        } catch (IOException e) {
            throw new IonException(e);
        }
        // If the input stream is growing, it is possible that fewer than BINARY_VERSION_MARKER_SIZE bytes are
        // available yet. Simply check whether the stream *could* contain binary Ion based on the available bytes.
        // If it can't, fall back to text.
        // NOTE: if incremental text reading is added, there will need to be logic that handles the case where
        // the reader is created with 0 bytes available, as it is impossible to determine text vs. binary without
        // reading at least one byte. Currently, in that case, just create a binary incremental reader. Either the
        // stream will always be empty (in which case it doesn't matter whether a text or binary reader is used)
        // or it's a binary stream (in which case the correct reader was created) or it's a growing text stream
        // (which has always been unsupported).
        if (startsWithGzipHeader(possibleIVM, bytesRead)) {
            try {
                ionData = new GZIPInputStream(
                    new SequenceInputStream(new ByteArrayInputStream(possibleIVM, 0, bytesRead), ionData)
                );
                bytesRead = ionData.read(possibleIVM);
            } catch (IOException e) {
                throw new IonException(e);
            }
        }
        if (startsWithIvm(possibleIVM, bytesRead)) {
            return binary.makeReader(builder, ionData, possibleIVM, 0, bytesRead);
        }
        InputStream wrapper;
        if (bytesRead > 0) {
            wrapper = new SequenceInputStream(
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
            _Private_IonReaderFactory::makeReaderText
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
