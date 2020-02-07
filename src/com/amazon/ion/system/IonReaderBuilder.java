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

package com.amazon.ion.system;

import static com.amazon.ion.impl._Private_IonReaderFactory.makeReader;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTextReader;
import com.amazon.ion.IonValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 * Build a new {@link IonReader} from the given {@link IonCatalog} and data
 * source. A data source is required, while an IonCatalog is optional. If no
 * IonCatalog is provided, an empty {@link SimpleCatalog} will be used.
 * <p>
 * {@link IonReader}s parse incrementally, so syntax errors in the input data
 * will not be detected as side effects of any of the {@code build} methods
 * in this class.
 */
@SuppressWarnings("deprecation")
public class IonReaderBuilder
{

    private IonCatalog catalog = null;

    private IonReaderBuilder()
    {
    }

    private IonReaderBuilder(IonReaderBuilder that)
    {
        this.catalog = that.catalog;
    }

    /**
     * The standard builder of {@link IonReader}s, with all configuration
     * properties having their default values.
     *
     * @return a new, mutable builder instance.
     */
    public static IonReaderBuilder standard()
    {
        return new Mutable();
    }

    /**
     * Creates a mutable copy of this builder.
     *
     * @return a new builder with the same configuration as {@code this}.
     */
    public IonReaderBuilder copy()
    {
        return new Mutable(this);
    }

    /**
     * Returns an immutable builder configured exactly like this one.
     *
     * @return this builder instance, if immutable;
     * otherwise an immutable copy of this builder.
     */
    public IonReaderBuilder immutable()
    {
        return this;
    }

    /**
     * Returns a mutable builder configured exactly like this one.
     *
     * @return this instance, if mutable;
     * otherwise a mutable copy of this instance.
     */
    public IonReaderBuilder mutable()
    {
        return copy();
    }

    /** NOT FOR APPLICATION USE! */
    protected void mutationCheck()
    {
        throw new UnsupportedOperationException("This builder is immutable");
    }

    /**
     * Declares the catalog to use when building an {@link IonReader},
     * returning a new mutable builder the current one is immutable.
     *
     * @param catalog the catalog to use in built readers.
     *  If null, a new {@link SimpleCatalog} will be used.
     *
     * @return this builder instance, if mutable;
     * otherwise a mutable copy of this builder.
     *
     * @see #setCatalog(IonCatalog)
     * @see #withCatalog(IonCatalog)
     */
    public IonReaderBuilder withCatalog(IonCatalog catalog)
    {
        IonReaderBuilder b = mutable();
        b.setCatalog(catalog);
        return b;
    }

    /**
     * Sets the catalog to use when building an {@link IonReader}.
     *
     * @param catalog the catalog to use in built readers.
     *  If null, a new {@link SimpleCatalog} will be used.
     *
     * @see #getCatalog()
     * @see #withCatalog(IonCatalog)
     *
     * @throws UnsupportedOperationException if this builder is immutable.
     */
    public void setCatalog(IonCatalog catalog)
    {
        mutationCheck();
        this.catalog = catalog;
    }

    /**
     * Gets the catalog to use when building an {@link IonReader}, or null
     * if none has been manually set. The catalog is needed to resolve shared
     * symbol table imports.
     *
     * @see #setCatalog(IonCatalog)
     * @see #withCatalog(IonCatalog)
     */
    public IonCatalog getCatalog()
    {
        return catalog;
    }

    private IonCatalog validateCatalog()
    {
        // matches behavior in IonSystemBuilder when no catalog provided
        return catalog != null ? catalog : new SimpleCatalog();
    }

    /**
     * Based on the builder's configuration properties, creates a new IonReader
     * instance over the given block of Ion data, detecting whether it's text or
     * binary data.
     * <p>
     * This method will auto-detect and uncompress GZIPped Ion data.
     *
     * @param ionData the source of the Ion data, which may be either Ion binary
     * data or UTF-8 Ion text. The reader retains a reference to the array, so
     * its data must not be modified while the reader is active. Must not be
     * null.
     *
     * @return a new {@link IonReader} instance; not {@code null}.
     *
     * @see IonSystem#newReader(byte[])
     */
    public IonReader build(byte[] ionData)
    {
        return makeReader(validateCatalog(), ionData);
    }

    /**
     * Based on the builder's configuration properties, creates a new IonReader
     * instance over the given block of Ion data, detecting whether it's text or
     * binary data.
     * <p>
     * This method will auto-detect and uncompress GZIPped Ion data.
     *
     * @param ionData the source of the Ion data, which is used only within the
     * range of bytes starting at {@code offset} for {@code len} bytes.
     * The data in that range may be either Ion binary data or UTF-8 Ion text.
     * The reader retains a reference to the array, so its data must not be
     * modified while the reader is active. Must not be null.
     * @param offset must be non-negative and less than {@code ionData.length}.
     * @param length must be non-negative and {@code offset+length} must not
     * exceed {@code ionData.length}.
     *
     * @see IonSystem#newReader(byte[], int, int)
     */
    public IonReader build(byte[] ionData, int offset, int length)
    {
        return makeReader(validateCatalog(), ionData, offset, length);
    }

    /**
     * Based on the builder's configuration properties, creates a new IonReader
     * instance over the given stream of Ion data, detecting whether it's text or
     * binary data.
     * <p>
     * This method will auto-detect and uncompress GZIPped Ion data.
     * <p>
     * Because this library performs its own buffering, it's recommended that
     * users avoid adding additional buffering to the given stream.
     *
     * @param ionData the source of the Ion data, which may be either Ion binary
     * data or UTF-8 Ion text. Must not be null.
     *
     * @return a new reader instance.
     * Callers must call {@link IonReader#close()} when finished with it.
     *
     * @throws IonException if the source throws {@link IOException}.
     *
     * @see IonSystem#newReader(InputStream)
     */
    public IonReader build(InputStream ionData)
    {
        return makeReader(validateCatalog(), ionData);
    }

    /**
     * Based on the builder's configuration properties, creates a new
     * {@link IonReader} instance over Ion text data.
     * <p>
     * Applications should generally use {@link #build(InputStream)}
     * whenever possible, since this library has much faster Unicode decoding
     * than the Java IO framework.
     * <p>
     * Because this library performs its own buffering, it's recommended that
     * you avoid adding additional buffering to the given stream.
     *
     * @param ionText the source of the Ion text data. Must not be null.
     *
     * @throws IonException if the source throws {@link IOException}.
     *
     * @see IonSystem#newReader(Reader)
     */
    public IonReader build(Reader ionText)
    {
        return makeReader(validateCatalog(), ionText);
    }

    /**
     * Based on the builder's configuration properties, creates a new
     * {@link IonReader} instance over an {@link IonValue} data model. Typically
     * this is used to iterate over a collection, such as an {@link IonStruct}.
     *
     * The given value and its children, if any, must not be modified until after
     * the IonReader constructed by this method is closed. Violating this
     * constraint results in undefined behavior.
     *
     * @param value must not be null.
     *
     * @see IonSystem#newReader(IonValue)
     */
    public IonReader build(IonValue value)
    {
        return makeReader(validateCatalog(), value);
    }

    /**
     * Based on the builder's configuration properties, creates an new
     * {@link IonReader} instance over Ion text data.
     *
     * @param ionText the source of the Ion text data. Must not be null.
     *
     * @see IonSystem#newReader(String)
     */
    public IonTextReader build(String ionText)
    {
        return makeReader(validateCatalog(), ionText);
    }

    private static class Mutable extends IonReaderBuilder
    {

        private Mutable()
        {
        }

        private Mutable(IonReaderBuilder that)
        {
            super(that);
        }

        @Override
        public IonReaderBuilder immutable()
        {
            return new IonReaderBuilder(this);
        }

        @Override
        public IonReaderBuilder mutable()
        {
            return this;
        }

        @Override
        protected void mutationCheck()
        {
        }

    }

}
