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

package com.amazon.ion.impl;

import static com.amazon.ion.impl._Private_Utils.initialSymtab;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.system.SimpleCatalog;
import com.amazon.ion.util._Private_FastAppendable;
import java.io.OutputStream;

/**
 * NOT FOR APPLICATION USE!
 */
public class _Private_IonTextWriterBuilder
    extends IonTextWriterBuilder
{
    private final static CharSequence SPACE_CHARACTER = " ";
    // TODO amzn/ion-java/issues/57 decide if this should be platform-specific
    private final static CharSequence LINE_SEPARATOR =
        System.getProperty("line.separator");


    public static _Private_IonTextWriterBuilder standard()
    {
        return new _Private_IonTextWriterBuilder.Mutable();
    }

    public static _Private_IonTextWriterBuilder STANDARD =
        standard().immutable();


    //=========================================================================

    private boolean _pretty_print;
    public boolean _blob_as_string;
    public boolean _clob_as_string;
    public boolean _decimal_as_float;
    public boolean _sexp_as_list;
    public boolean _skip_annotations;
    public boolean _string_as_json;
    public boolean _symbol_as_string;
    public boolean _timestamp_as_millis;
    public boolean _timestamp_as_string;
    public boolean _untyped_nulls;
    private _Private_CallbackBuilder _callback_builder;


    private _Private_IonTextWriterBuilder()
    {
        super();
    }

    private _Private_IonTextWriterBuilder(_Private_IonTextWriterBuilder that)
    {
        super(that);
        this._callback_builder    = that._callback_builder   ;
        this._pretty_print        = that._pretty_print       ;
        this._blob_as_string      = that._blob_as_string     ;
        this._clob_as_string      = that._clob_as_string     ;
        this._decimal_as_float    = that._decimal_as_float   ;
        this._sexp_as_list        = that._sexp_as_list       ;
        this._skip_annotations    = that._skip_annotations   ;
        this._string_as_json      = that._string_as_json     ;
        this._symbol_as_string    = that._symbol_as_string   ;
        this._timestamp_as_millis = that._timestamp_as_millis;
        this._timestamp_as_string = that._timestamp_as_string;
        this._untyped_nulls       = that._untyped_nulls      ;
    }


    @Override
    public final _Private_IonTextWriterBuilder copy()
    {
        return new Mutable(this);
    }

    @Override
    public _Private_IonTextWriterBuilder immutable()
    {
        return this;
    }

    @Override
    public _Private_IonTextWriterBuilder mutable()
    {
        return copy();
    }


    //=========================================================================

    @Override
    public final IonTextWriterBuilder withPrettyPrinting()
    {
        _Private_IonTextWriterBuilder b = mutable();
        b._pretty_print = true;
        return b;
    }

    @Override
    public final IonTextWriterBuilder withJsonDowngrade()
    {
        _Private_IonTextWriterBuilder b = mutable();

        b.withMinimalSystemData();

        _blob_as_string      = true;
        _clob_as_string      = true;
        // datagramAsList    = true; // TODO
        _decimal_as_float    = true;
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


    final boolean isPrettyPrintOn()
    {
        return _pretty_print;
    }

    final CharSequence lineSeparator()
    {
        if (_pretty_print) {
            return LINE_SEPARATOR;
        }
        else {
            return SPACE_CHARACTER;
        }
    }


    //=========================================================================

    private _Private_IonTextWriterBuilder fillDefaults()
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

        return (_Private_IonTextWriterBuilder) b.immutable();
    }


    /** Assumes that {@link #fillDefaults()} has been called. */
    private IonWriter build(_Private_FastAppendable appender)
    {
        IonCatalog catalog = getCatalog();
        SymbolTable[] imports = getImports();

        // TODO We shouldn't need a system here
        IonSystem system =
            IonSystemBuilder.standard().withCatalog(catalog).build();

        SymbolTable defaultSystemSymtab = system.getSystemSymbolTable();

        IonWriterSystemText systemWriter =
            (getCallbackBuilder() == null
                ? new IonWriterSystemText(defaultSystemSymtab,
                                          this,
                                          appender)
                : new IonWriterSystemTextMarkup(defaultSystemSymtab,
                                                this,
                                                appender));

        SymbolTable initialSymtab =
            initialSymtab(((_Private_ValueFactory)system).getLstFactory(), defaultSystemSymtab, imports);

        return new IonWriterUser(catalog, system, systemWriter, initialSymtab);
    }


    @Override
    public final IonWriter build(Appendable out)
    {
        _Private_IonTextWriterBuilder b = fillDefaults();

        _Private_FastAppendable fast = new AppendableFastAppendable(out);

        return b.build(fast);
    }


    @Override
    public final IonWriter build(OutputStream out)
    {
        _Private_IonTextWriterBuilder b = fillDefaults();

        _Private_FastAppendable fast = new OutputStreamFastAppendable(out);

        return b.build(fast);
    }

    //=========================================================================

    private static final class Mutable
        extends _Private_IonTextWriterBuilder
    {
        private Mutable() { }

        private Mutable(_Private_IonTextWriterBuilder that)
        {
            super(that);
        }

        @Override
        public _Private_IonTextWriterBuilder immutable()
        {
            return new _Private_IonTextWriterBuilder(this);
        }

        @Override
        public _Private_IonTextWriterBuilder mutable()
        {
            return this;
        }

        @Override
        protected void mutationCheck()
        {
        }
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
    public final _Private_IonTextWriterBuilder
                 withCallbackBuilder(_Private_CallbackBuilder builder)
    {
        _Private_IonTextWriterBuilder b = mutable();
        b.setCallbackBuilder(builder);
        return b;
    }
}
