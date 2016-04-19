/*
 * Copyright 2011-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion.impl;

import static software.amazon.ion.impl.PrivateUtils.initialSymtab;

import java.io.OutputStream;
import software.amazon.ion.IonCatalog;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.system.IonSystemBuilder;
import software.amazon.ion.system.IonTextWriterBuilder;
import software.amazon.ion.system.SimpleCatalog;
import software.amazon.ion.util.PrivateFastAppendable;

/**
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public class PrivateIonTextWriterBuilder
    extends IonTextWriterBuilder
{
    private final static CharSequence SPACE_CHARACTER = " ";
    // TODO amznlabs/ion-java#57 decide if this should be platform-specific
    private final static CharSequence LINE_SEPARATOR =
        System.getProperty("line.separator");


    public static PrivateIonTextWriterBuilder standard()
    {
        return new PrivateIonTextWriterBuilder.Mutable();
    }

    public static PrivateIonTextWriterBuilder STANDARD =
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

    private PrivateIonTextWriterBuilder()
    {
        super();
    }

    private PrivateIonTextWriterBuilder(PrivateIonTextWriterBuilder that)
    {
        super(that);
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
    public final PrivateIonTextWriterBuilder copy()
    {
        return new Mutable(this);
    }

    @Override
    public PrivateIonTextWriterBuilder immutable()
    {
        return this;
    }

    @Override
    public PrivateIonTextWriterBuilder mutable()
    {
        return copy();
    }


    //=========================================================================

    @Override
    public final IonTextWriterBuilder withPrettyPrinting()
    {
        PrivateIonTextWriterBuilder b = mutable();
        b._pretty_print = true;
        return b;
    }

    @Override
    public final IonTextWriterBuilder withJsonDowngrade()
    {
        PrivateIonTextWriterBuilder b = mutable();

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

    private PrivateIonTextWriterBuilder fillDefaults()
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

        return (PrivateIonTextWriterBuilder) b.immutable();
    }


    /** Assumes that {@link #fillDefaults()} has been called. */
    private IonWriter build(PrivateFastAppendable appender)
    {
        IonCatalog catalog = getCatalog();
        SymbolTable[] imports = getImports();

        // TODO We shouldn't need a system here
        IonSystem system =
            IonSystemBuilder.standard().withCatalog(catalog).build();

        SymbolTable defaultSystemSymtab = system.getSystemSymbolTable();

        IonWriterSystemText systemWriter = new IonWriterSystemText(defaultSystemSymtab,
                                                                   this,
                                                                   appender);

        SymbolTable initialSymtab =
            initialSymtab(system, defaultSystemSymtab, imports);

        return new IonWriterUser(catalog, system, systemWriter, initialSymtab);
    }


    @Override
    public final IonWriter build(Appendable out)
    {
        PrivateIonTextWriterBuilder b = fillDefaults();

        PrivateFastAppendable fast = new AppendableFastAppendable(out);

        return b.build(fast);
    }


    @Override
    public final IonWriter build(OutputStream out)
    {
        PrivateIonTextWriterBuilder b = fillDefaults();

        PrivateFastAppendable fast = new OutputStreamFastAppendable(out);

        return b.build(fast);
    }

    //=========================================================================

    private static final class Mutable
        extends PrivateIonTextWriterBuilder
    {
        private Mutable() { }

        private Mutable(PrivateIonTextWriterBuilder that)
        {
            super(that);
        }

        @Override
        public PrivateIonTextWriterBuilder immutable()
        {
            return new PrivateIonTextWriterBuilder(this);
        }

        @Override
        public PrivateIonTextWriterBuilder mutable()
        {
            return this;
        }

        @Override
        protected void mutationCheck()
        {
        }
    }
}
