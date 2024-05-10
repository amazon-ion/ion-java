// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;


import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.system.SimpleCatalog;

/**
 * Contains configuration common to text writers for all Ion versions.
 * NOT FOR APPLICATION USE!
 */
public abstract class _Private_IonTextWriterBuilder<T extends _Private_IonTextWriterBuilder<T>>
    extends IonTextWriterBuilder
{
    private final static CharSequence SPACE_CHARACTER = " ";


    //=========================================================================

    public boolean _pretty_print;

    // These options control whether the IonTextWriter will write standard ion or ion that is down-converted json.
    public boolean _blob_as_string;
    public boolean _clob_as_string;
    public boolean _decimal_as_float;
    public boolean _float_nan_and_inf_as_null;
    public boolean _sexp_as_list;
    public boolean _skip_annotations;
    public boolean _string_as_json;
    public boolean _symbol_as_string;
    public boolean _timestamp_as_millis;
    public boolean _timestamp_as_string;
    public boolean _untyped_nulls;
    public boolean _allow_invalid_sids;

    public _Private_CallbackBuilder _callback_builder;


    _Private_IonTextWriterBuilder()
    {
        super();
    }

    _Private_IonTextWriterBuilder(T that)
    {
        super(that);
        this._callback_builder    = that._callback_builder   ;
        this._pretty_print        = that._pretty_print       ;
        this._blob_as_string      = that._blob_as_string     ;
        this._clob_as_string      = that._clob_as_string     ;
        this._decimal_as_float    = that._decimal_as_float   ;
        this._float_nan_and_inf_as_null = that._float_nan_and_inf_as_null;
        this._sexp_as_list        = that._sexp_as_list       ;
        this._skip_annotations    = that._skip_annotations   ;
        this._string_as_json      = that._string_as_json     ;
        this._symbol_as_string    = that._symbol_as_string   ;
        this._timestamp_as_millis = that._timestamp_as_millis;
        this._timestamp_as_string = that._timestamp_as_string;
        this._untyped_nulls       = that._untyped_nulls      ;
        this._allow_invalid_sids  = that._allow_invalid_sids ;
    }

    @Override
    public abstract T copy();

    @Override
    public T immutable()
    {
        return (T) this;
    }

    @Override
    public T mutable()
    {
        return copy();
    }

    //=========================================================================

    @Override
    public IonTextWriterBuilder withPrettyPrinting()
    {
        T b = mutable();
        b._pretty_print = true;
        return b;
    }

    @Override
    public IonTextWriterBuilder withJsonDowngrade()
    {
        T b = mutable();

        b.withMinimalSystemData();

        _blob_as_string      = true;
        _clob_as_string      = true;
        // datagramAsList    = true; // TODO
        _decimal_as_float    = true;
        _float_nan_and_inf_as_null = true;
        _sexp_as_list        = true;
        _skip_annotations    = true;
        // skipSystemValues  = true; // TODO
        _string_as_json      = true;
        _symbol_as_string    = true;
        _timestamp_as_string = true;  // TODO different from Printer
        _timestamp_as_millis = false;
        _untyped_nulls       = true;

        return b;
    }

    /**
     * Determines whether this builder should allow invalid SIDs (i.e. SIDs out of range of the current symbol table or
     * SIDs with unknown text). This is disabled by default, because enabling this option can result in writing invalid
     * Ion data. This option should only be enabled when writing text Ion data primarily for debugging by humans.
     * @param allowInvalidSids whether to allow invalid SIDs.
     * @return the builder.
     */
    public final T withInvalidSidsAllowed(boolean allowInvalidSids) {
        T b = mutable();
        b._allow_invalid_sids = allowInvalidSids;
        return b;
    }


    final boolean isPrettyPrintOn()
    {
        return _pretty_print;
    }

    final CharSequence lineSeparator()
    {
        if (_pretty_print) {
            return getNewLineType().getCharSequence();
        }
        else {
            return SPACE_CHARACTER;
        }
    }

    final CharSequence topLevelSeparator()
    {
        return getWriteTopLevelValuesOnNewLines() ? getNewLineType().getCharSequence() : lineSeparator();
    }

    //=========================================================================

    T fillDefaults()
    {
        // Ensure that we don't modify the user's builder.
        IonTextWriterBuilder b = copy();

        if (b.getCatalog() == null)
        {
            b.setCatalog(new SimpleCatalog());
        }

        if (b.getCharset() == null)
        {
            b.setCharset(UTF8);
        }

        if (b.getNewLineType() == null)
        {
            b.setNewLineType(NewLineType.PLATFORM_DEPENDENT);
        }

        if (b.getMaximumTimestampPrecisionDigits() < 1) {
            throw new IllegalArgumentException(String.format(
                "Configured maximum timestamp precision must be positive, not %d.",
                b.getMaximumTimestampPrecisionDigits()
            ));
        }

        return (T) b.immutable();
    }

    //-------------------------------------------------------------------------

    /**
     * Gets the {@link _Private_CallbackBuilder} that will be used to create a
     * {@link _Private_MarkupCallback} when a new writer is built.
     * @return The builder that will be used to build a new MarkupCallback.
     * @see #setCallbackBuilder(_Private_CallbackBuilder)
     * @see #withCallbackBuilder(_Private_CallbackBuilder)
     */
    public final _Private_CallbackBuilder getCallbackBuilder()
    {
        return this._callback_builder;
    }

    /**
     * Sets the {@link _Private_CallbackBuilder} that will be used to create a
     * {@link _Private_MarkupCallback} when a new writer is built.
     * @param builder
     *            The builder that will be used to build a new MarkupCallback.
     * @see #getCallbackBuilder()
     * @see #withCallbackBuilder(_Private_CallbackBuilder)
     * @throws UnsupportedOperationException
     *             if this is immutable.
     */
    public void setCallbackBuilder(_Private_CallbackBuilder builder)
    {
        mutationCheck();
        this._callback_builder = builder;
    }

    /**
     * Declares the {@link _Private_CallbackBuilder} to use when building.
     * @param builder
     *            The builder that will be used to build a new MarkupCallback.
     * @return this instance, if mutable; otherwise a mutable copy of this
     *         instance.
     * @see #getCallbackBuilder()
     * @see #setCallbackBuilder(_Private_CallbackBuilder)
     */
    public final T withCallbackBuilder(_Private_CallbackBuilder builder)
    {
        T b = mutable();
        b.setCallbackBuilder(builder);
        return b;
    }
}
