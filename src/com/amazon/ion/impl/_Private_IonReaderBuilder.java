package com.amazon.ion.impl;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonTextReader;
import com.amazon.ion.IonValue;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ion.util.IonStreamUtils;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import static com.amazon.ion.impl.LocalSymbolTable.DEFAULT_LST_FACTORY;
import static com.amazon.ion.impl._Private_IonReaderFactory.makeIncrementalReader;
import static com.amazon.ion.impl._Private_IonReaderFactory.makeReader;

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

    @Override
    public IonReader build(byte[] ionData, int offset, int length)
    {
        if (isIncrementalReadingEnabled()) {
            if (IonStreamUtils.isGzip(ionData, offset, length)) {
                throw new IllegalArgumentException("Automatic GZIP detection is not supported with incremental" +
                    "support enabled. Wrap the bytes with a GZIPInputStream and call build(InputStream).");
            }
            if (IonStreamUtils.isIonBinary(ionData, offset, length)) {
                return makeIncrementalReader(this, new ByteArrayInputStream(ionData, offset, length));
            }
        }
        return makeReader(validateCatalog(), ionData, offset, length, lstFactory);
    }

    /**
     * Determines whether a stream that begins with the bytes in the provided buffer could be binary Ion.
     * @param buffer up to the first four bytes in a stream.
     * @param length the actual number of bytes in the buffer.
     * @return true if the first 'length' bytes in 'buffer' match the first 'length' bytes in the binary IVM.
     */
    private static boolean startsWithIvm(byte[] buffer, int length) {
        for (int i = 0; i < length; i++) {
            if (_Private_IonConstants.BINARY_VERSION_MARKER_1_0[i] != buffer[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public IonReader build(InputStream ionData)
    {
        InputStream wrapper = ionData;
        if (isIncrementalReadingEnabled()) {
            if (!ionData.markSupported()) {
                wrapper = new BufferedInputStream(ionData);
            }
            wrapper.mark(_Private_IonConstants.BINARY_VERSION_MARKER_SIZE);
            byte[] possibleIVM = new byte[_Private_IonConstants.BINARY_VERSION_MARKER_SIZE];
            int bytesRead;
            try {
                bytesRead = wrapper.read(possibleIVM);
                wrapper.reset();
            } catch (IOException e) {
                throw new IonException(e);
            }
            if (IonStreamUtils.isGzip(possibleIVM, 0, possibleIVM.length)) {
                throw new IllegalArgumentException("Automatic GZIP detection is not supported with incremental" +
                    "support enabled. Wrap the bytes with a GZIPInputStream and call build(InputStream).");
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
            if (startsWithIvm(possibleIVM, bytesRead)) {
                return makeIncrementalReader(this, wrapper);
            }
        }
        return makeReader(validateCatalog(), wrapper, lstFactory);
    }

    @Override
    public IonReader build(Reader ionText) {
        return makeReader(validateCatalog(), ionText, lstFactory);
    }

    @Override
    public IonReader build(IonValue value) {
        return makeReader(validateCatalog(), value, lstFactory);
    }

    @Override
    public IonTextReader build(String ionText) {
        return makeReader(validateCatalog(), ionText, lstFactory);
    }

}
