// Copyright (c) 2011 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.system.SimpleCatalog;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 *
 */
public class _Private_IonTextWriterBuilder
    extends IonTextWriterBuilder
{
    private final static CharSequence SPACE_CHARACTER = " ";
    /** TODO shouldn't be platform-specific */
    private final static CharSequence LINE_SEPARATOR = System.getProperty("line.separator");


    public static _Private_IonTextWriterBuilder standard()
    {
        return new _Private_IonTextWriterBuilder();
    }

    public static IonTextWriterBuilder simplifiedAscii()
    {
        _Private_IonTextWriterBuilder b = new _Private_IonTextWriterBuilder();
        b.setCharset(ASCII);
        b._filter_symbol_tables = true;
        return b;
    }

    //=========================================================================

    public boolean       _pretty_print;
    public boolean       _filter_symbol_tables;
    public boolean       _suppress_ion_version_marker;

    /**
     * Strings and clobs longer than this length will be rendered as
     * long-strings, but will only line-break on extant '\n' code points.
     */
    public int _long_string_threshold = Integer.MAX_VALUE;

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


    /**
     *
     */
    public _Private_IonTextWriterBuilder()
    {
        super();
    }

    public _Private_IonTextWriterBuilder(_Private_IonTextWriterBuilder that)
    {
        super(that);

        this._pretty_print                = that._pretty_print               ;
        this._filter_symbol_tables        = that._filter_symbol_tables       ;
        this._suppress_ion_version_marker = that._suppress_ion_version_marker;
        this._long_string_threshold       = that._long_string_threshold      ;
        this._blob_as_string              = that._blob_as_string             ;
        this._clob_as_string              = that._clob_as_string             ;
        this._decimal_as_float            = that._decimal_as_float           ;
        this._sexp_as_list                = that._sexp_as_list               ;
        this._skip_annotations            = that._skip_annotations           ;
        this._string_as_json              = that._string_as_json             ;
        this._symbol_as_string            = that._symbol_as_string           ;
        this._timestamp_as_millis         = that._timestamp_as_millis        ;
        this._timestamp_as_string         = that._timestamp_as_string        ;
        this._untyped_nulls               = that._untyped_nulls              ;
    }


    @Override
    public IonTextWriterBuilder immutable()
    {
        return new Immutable(this);
    }


    //=========================================================================


    final boolean isPrettyPrintOn()
    {
        return _pretty_print;
    }

    public final boolean isFilteringSymbolTables()
    {
        return _filter_symbol_tables;
    }

    public final boolean isSuppressingInitialIvm()
    {
        return this._suppress_ion_version_marker;
    }

    public final CharSequence lineSeparator() {
        if (_pretty_print) {
            return LINE_SEPARATOR;
        }
        else {
            return SPACE_CHARACTER;
        }
    }


    //=========================================================================

    _Private_IonTextWriterBuilder fillDefaults()
    {
        IonTextWriterBuilder b = this;
        if (b.getCatalog() == null)
        {
            b = b.withCatalog(new SimpleCatalog());
        }

        return (_Private_IonTextWriterBuilder) b.immutable();
    }

    @Override
    public final IonWriter build(Appendable out)
    {
        _Private_IonTextWriterBuilder b = fillDefaults();
        IonCatalog catalog = b.getCatalog();

        // TODO We shouldn't need a system here
        IonSystem system =
            IonSystemBuilder.standard().withCatalog(catalog).build();

        IonWriterSystemText systemWriter =
            new IonWriterSystemText(system.getSystemSymbolTable(), b, out);

        return new IonWriterUserText(system, systemWriter);
    }

    @Override
    public final IonWriter build(OutputStream out)
    {
        _Private_IonTextWriterBuilder b = fillDefaults();
        IonCatalog catalog = b.getCatalog();

        // TODO We shouldn't need a system here
        IonSystem system =
            IonSystemBuilder.standard().withCatalog(catalog).build();

        IonWriterSystemText systemWriter =
            new IonWriterSystemText(system.getSystemSymbolTable(), b, out);

        return new IonWriterUserText(system, systemWriter);
    }

    //=========================================================================

    private static final class Immutable
        extends _Private_IonTextWriterBuilder
    {
        private Immutable(_Private_IonTextWriterBuilder that)
        {
            super(that);
        }

        @Override
        public IonTextWriterBuilder immutable()
        {
            return this;
        }

        @Override
        public IonTextWriterBuilder mutable()
        {
            return new _Private_IonTextWriterBuilder(this);
        }


        private void mutationFailure()
        {
            throw new UnsupportedOperationException("This builder is immutable");
        }

        @Override
        public void setCatalog(IonCatalog catalog)
        {
            mutationFailure();
        }

        @Override
        public void setCharset(Charset charset)
        {
            mutationFailure();
        }

        @Override
        public void setImports(SymbolTable... imports)
        {
            mutationFailure();
        }
    }
}
