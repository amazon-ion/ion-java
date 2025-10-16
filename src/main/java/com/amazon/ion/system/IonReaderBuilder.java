// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import com.amazon.ion.util.GzipStreamInterceptor;
import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonTextReader;
import com.amazon.ion.IonValue;
import com.amazon.ion.util.InputStreamInterceptor;
import com.amazon.ion.impl._Private_IonReaderBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Build a new {@link IonReader} from the given {@link IonCatalog} and data
 * source. A data source is required, while an IonCatalog is optional. If no
 * IonCatalog is provided, an empty {@link SimpleCatalog} will be used.
 * <p>
 * {@link IonReader}s parse incrementally, so syntax errors in the input data
 * will not be detected as side effects of any of the {@code build} methods
 * in this class.
 * 
 * <h2>Obtaining and Usage</h2>
 * An {@code IonReaderBuilder} with the default configuration may be constructed
 * as follows. This builder will construct {@code IonReader} instances which
 * can read both text and binary Ion data and is appropriate for simple use
 * cases or when no IonCatalog is in use.
 * {@snippet :
 * IonReaderBuilder readerBuilder = IonReaderBuilder.standard();
 * }
 * {@code IonReaderBuilder}s can be configured by chaining calls to {@code with*()}
 * configuration methods. Below is an example of a builder configured to
 * incrementally read Ion binary data, use a custom initial buffer size, throw
 * on inputs that would require a buffer over a specified size, and use a
 * user-provided IonCatalog.
 * {@snippet :
 * // Create an IonCatalog and IonBufferConfiguration to use with IonReaderBuilder
 * final IonCatalog catalog = new SimpleCatalog();
 * final IonBufferConfiguration bufferConfiguration = IonBufferConfiguration.Builder.standard()
 *       .onOversizedSymbolTable(() -> { throw new IllegalStateException("Oversized system table encountered"); })
 *       .onOversizedValue(() -> { throw new IllegalStateException("Oversized value encountered"); })
 *       .withInitialBufferSize(1024)
 *       .withMaximumBufferSize(1024 * 1024)
 *       .build();
 *
 * IonReaderBuilder readerBuilder = IonReaderBuilder.standard()
 *     .withIncrementalReadingEnabled(true)
 *     .withBufferConfiguration(bufferConfiguration)
 *     .withCatalog(catalog);
 * }
 * 
 * <h3>Building a Reader over a Data Source</h3>
 * An {@code IonReader} may be obtained from a builder by calling {@code build}
 * over the appropriate data source. Below is an example of a reader being constructed
 * over a string containing Ion text from a builder with the default configuration.
 * {@snippet :
 * IonReaderBuilder readerBuilder = IonReaderBuilder.standard();
 * final String helloWorld = "{hello: \"world\"}";
 * try (final IonReader reader = readerBuilder.build(helloWorld)) {
 *     reader.next();
 *     reader.stepIn();
 *     reader.next();
 *     System.out.println(reader.getFieldName() + " " + reader.stringValue());  // prints "hello world"
 * }
 * }
 * Builders can build an IonReader over a string, {@code byte[]} array,
 * {@link java.io.InputStream}, {@link java.io.Reader}, or existing {@link com.amazon.ion.IonValue}
 * data model. Building a reader over a byte array allows specifying the start
 * index and length of the data to be read. Readers built over {@code byte[]}
 * arrays or {@code InputStream}s are capable of reading both binary and text
 * Ion; readers built over strings and {@code Reader} instances are only capable of
 * reading text Ion.
 */
@SuppressWarnings("deprecation")
public abstract class IonReaderBuilder
{

    // Default stream interceptors, which always begin with the GZIP interceptor.
    private static final List<InputStreamInterceptor> DEFAULT_STREAM_INTERCEPTORS = Collections.singletonList(GzipStreamInterceptor.INSTANCE);

    // Stream interceptors detected by the ClassLoader.
    // Note: each ClassLoader may have access to a different list of detected interceptors, but static initialization
    // is performed once per load of the class, and is guaranteed thread-safe. Therefore, there is no need to manually
    // maintain a thread-safe cache of detected interceptors.
    private static final List<InputStreamInterceptor> DETECTED_STREAM_INTERCEPTORS = Collections.unmodifiableList(detectStreamInterceptorsOnClasspath());


    private IonCatalog catalog = null;
    private boolean isIncrementalReadingEnabled = false;
    private IonBufferConfiguration bufferConfiguration = IonBufferConfiguration.DEFAULT;
    private List<InputStreamInterceptor> streamInterceptors = null;

    protected IonReaderBuilder()
    {
    }

    protected IonReaderBuilder(IonReaderBuilder that)
    {
        this.catalog = that.catalog;
        this.isIncrementalReadingEnabled = that.isIncrementalReadingEnabled;
        this.bufferConfiguration = that.bufferConfiguration;
        this.streamInterceptors = that.streamInterceptors == null ? null : new ArrayList<>(that.streamInterceptors);
    }

    /**
     * The standard builder of {@link IonReader}s, with all configuration
     * properties having their default values.
     *
     * @return a new, mutable builder instance.
     */
    public static IonReaderBuilder standard()
    {
        return new _Private_IonReaderBuilder.Mutable();
    }

    /**
     * Creates a mutable copy of this builder.
     *
     * @return a new builder with the same configuration as {@code this}.
     */
    public IonReaderBuilder copy()
    {
        return new _Private_IonReaderBuilder.Mutable(this);
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

    protected IonCatalog validateCatalog()
    {
        // matches behavior in IonSystemBuilder when no catalog provided
        return catalog != null ? catalog : new SimpleCatalog();
    }

    /**
     * <p>
     * Determines whether the IonReader will allow incremental reading of binary Ion data. When enabled, if
     * {@link IonReader#next()} returns {@code null} at the top-level, it indicates that there is not enough data
     * in the stream to complete a top-level value. The user may wait for more data to become available in the stream
     * and call {@link IonReader#next()} again to continue reading. Unlike the non-incremental reader, the incremental
     * reader will never throw an exception due to unexpected EOF during {@code next()}. If, however,
     * {@link IonReader#close()} is called when an incomplete value is buffered, an {@link IonException} will be raised.
     * </p>
     * <p>
     * There is currently no incremental text IonReader, so for text data a non-incremental IonReader will be
     * returned regardless of the value of this option. If incremental text reading is supported in the future, it
     * may be enabled via this option.
     * </p>
     * <p>
     * There is one caveat to note when using this option: the incremental implementation must be able to buffer an
     * entire top-level value in memory. This will not be a problem for the vast majority of Ion streams, as it is rare
     * for a single top-level value or symbol table to exceed a few megabytes in size. However, if the size of the
     * stream's values risks exceeding the available memory, then this option must not be enabled.
     * </p>
     * @param isEnabled true if the option is enabled; otherwise, false.
     *
     * @return this builder instance, if mutable;
     * otherwise a mutable copy of this builder.
     *
     * @see #setIncrementalReadingEnabled()
     * @see #setIncrementalReadingDisabled()
     */
    public IonReaderBuilder withIncrementalReadingEnabled(boolean isEnabled) {
        IonReaderBuilder b = mutable();
        if (isEnabled) {
            b.setIncrementalReadingEnabled();
        } else {
            b.setIncrementalReadingDisabled();
        }
        return b;
    }

    /**
     * @see #withIncrementalReadingEnabled(boolean)
     */
    public void setIncrementalReadingEnabled() {
        mutationCheck();
        isIncrementalReadingEnabled = true;
    }

    /**
     * @see #withIncrementalReadingEnabled(boolean)
     */
    public void setIncrementalReadingDisabled() {
        mutationCheck();
        isIncrementalReadingEnabled = false;
    }

    /**
     * @see #withIncrementalReadingEnabled(boolean)
     * @return true if incremental reading is enabled; otherwise, false.
     */
    public boolean isIncrementalReadingEnabled() {
        return isIncrementalReadingEnabled;
    }

    /**
     * Sets the buffer configuration. This can be used, for example, to set a maximum buffer size
     * and receive notifications when values would exceed this size. Currently, this is ignored unless incremental
     * reading has been enabled via {@link #withIncrementalReadingEnabled(boolean)}) or
     * {@link #setIncrementalReadingEnabled()}. This configuration is optional. If not provided, the buffer size will
     * be limited only by the available memory.
     *
     * @param configuration the configuration.
     *
     * @return this builder instance, if mutable;
     * otherwise a mutable copy of this builder.
     * 
     * @see #setBufferConfiguration(IonBufferConfiguration)
     */
    public IonReaderBuilder withBufferConfiguration(IonBufferConfiguration configuration) {
        IonReaderBuilder b = mutable();
        b.setBufferConfiguration(configuration);
        return b;
    }

    /**
     * @see #withBufferConfiguration(IonBufferConfiguration)
     */
    public void setBufferConfiguration(IonBufferConfiguration configuration) {
        mutationCheck();
        if (configuration == null) {
            throw new IllegalArgumentException("Configuration must not be null. To use the default configuration, provide IonBufferConfiguration.DEFAULT.");
        }
        bufferConfiguration = configuration;
    }

    /**
     * @see #withBufferConfiguration(IonBufferConfiguration)
     * @return the current configuration.
     */
    public IonBufferConfiguration getBufferConfiguration() {
        return bufferConfiguration;
    }

    /**
     * Adds an {@link InputStreamInterceptor} to the end of the list that the builder will attempt
     * to apply to a stream before creating {@link IonReader} instances over that stream.
     * {@link GzipStreamInterceptor} is always consulted first, and need not be added. The first
     * interceptor in the list that matches the stream will be used; if any chaining of interceptors
     * is required, it is up to the caller to provide a custom interceptor implementation to
     * achieve this.
     * <p>
     * Users may also or instead register implementations as service providers on the classpath.
     * See {@link ServiceLoader} for details about how to do this.
     * <p>
     * The list of stream interceptors available to the reader always begins with
     * {@link GzipStreamInterceptor} and is followed by:
     * <ol>
     *     <li>any stream interceptors detected on the classpath using
     *     {@link ServiceLoader#load(Class)}, then</li>
     *     <li>any stream interceptor(s) added by calling this method.</li>
     * </ol>
     *
     * @param streamInterceptor the stream interceptor to add.
     *
     * @return this builder instance, if mutable;
     * otherwise a mutable copy of this builder.
     */
    public IonReaderBuilder addInputStreamInterceptor(InputStreamInterceptor streamInterceptor) {
        IonReaderBuilder b = mutable();
        if (b.streamInterceptors == null) {
            // 4 is arbitrary, but more would be very rare.
            b.streamInterceptors = new ArrayList<>(DETECTED_STREAM_INTERCEPTORS.size() + 4);
            b.streamInterceptors.addAll(DETECTED_STREAM_INTERCEPTORS);
        }
        b.streamInterceptors.add(streamInterceptor);
        return b;
    }

    /**
     * Detects implementations of {@link InputStreamInterceptor} available to the {@link ClassLoader} that loaded
     * this class, appending any implementations found to the list of stream interceptors enabled by default.
     * @return the stream interceptors.
     */
    private static List<InputStreamInterceptor> detectStreamInterceptorsOnClasspath() {
        ServiceLoader<InputStreamInterceptor> loader = ServiceLoader.load(
            InputStreamInterceptor.class,
            // The ClassLoader used to load this class. Each ClassLoader may have access to different resources.
            IonReaderBuilder.class.getClassLoader()
        );
        Iterator<InputStreamInterceptor> interceptorIterator = loader.iterator();
        if (!interceptorIterator.hasNext()) {
            // Avoid allocating a new list in the common case: no custom interceptors detected.
            return DEFAULT_STREAM_INTERCEPTORS;
        }
        List<InputStreamInterceptor> interceptorsOnClasspath = new ArrayList<>(4); // 4 is arbitrary, but more would be very rare.
        interceptorsOnClasspath.addAll(DEFAULT_STREAM_INTERCEPTORS);
        interceptorIterator.forEachRemaining(interceptorsOnClasspath::add);
        return interceptorsOnClasspath;
    }

    /**
     * Gets the {@link InputStreamInterceptor} instances available to this builder. The returned list will always begin
     * with the default stream interceptor, which detects GZIP. Any stream interceptor(s) detected on the classpath
     * by {@link ServiceLoader#load(Class)} will immediately follow. Any stream interceptor(s) manually added using
     * {@link #addInputStreamInterceptor(InputStreamInterceptor)} will occur at the end of the list.
     * @return an unmodifiable view of the stream interceptors currently configured.
     */
    public List<InputStreamInterceptor> getInputStreamInterceptors() {
        if (streamInterceptors == null) {
            return DETECTED_STREAM_INTERCEPTORS;
        }
        return Collections.unmodifiableList(streamInterceptors);
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
        return build(ionData, 0, ionData.length);
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
    public abstract IonReader build(byte[] ionData, int offset, int length);

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
    public abstract IonReader build(InputStream ionData);

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
    public abstract IonReader build(Reader ionText);

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
    public abstract IonReader build(IonValue value);

    /**
     * Based on the builder's configuration properties, creates an new
     * {@link IonReader} instance over Ion text data.
     *
     * @param ionText the source of the Ion text data. Must not be null.
     *
     * @see IonSystem#newReader(String)
     */
    public abstract IonTextReader build(String ionText);

}
