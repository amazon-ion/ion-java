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

/**
 *
 */
public class _Private_IonTextWriterBuilder
    extends IonTextWriterBuilder
{
    public static IonTextWriterBuilder standard()
    {
        return new _Private_IonTextWriterBuilder();
    }

    public static IonTextWriterBuilder simplifiedAscii()  // TODO test
    {
        _Private_TextOptions options =
            new _Private_TextOptions(false /* prettyPrint */,
                                     true /* printAscii */,
                                     true /* filterOutSymbolTables */);

        _Private_IonTextWriterBuilder b = new _Private_IonTextWriterBuilder();
        b.setOptions(options);
        return b;
    }

    //=========================================================================

    private _Private_TextOptions myOptions;

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
        this.myOptions = that.myOptions;  // TODO clone?
    }


    @Override
    public IonTextWriterBuilder immutable()
    {
        return new Immutable(this);
    }


    //=========================================================================


    protected _Private_TextOptions getOptions()
    {
        return myOptions;
    }

    protected void setOptions(_Private_TextOptions options)
    {
        myOptions = options; // TODO copy
    }

    public IonTextWriterBuilder withOptions(_Private_TextOptions options)
    {
        _Private_IonTextWriterBuilder b =
            (_Private_IonTextWriterBuilder) mutable();
        b.setOptions(options); // TODO copy
        return b;
    }

    private _Private_TextOptions effectiveOptions()
    {
        _Private_TextOptions options = getOptions();
        if (options != null) return options;

        return new _Private_TextOptions(false /* prettyPrint */,
                                        true /* printAscii */,
                                        true /* filterOutSymbolTables */);
    }



    //=========================================================================


    @Override
    public final IonWriter build(Appendable out)
    {
        IonCatalog catalog = getCatalog();
        if (catalog == null) catalog = new SimpleCatalog();

        // TODO We shouldn't need a system here
        IonSystem system =
            IonSystemBuilder.standard().withCatalog(catalog).build();

        _Private_TextOptions options = effectiveOptions();

        IonWriterSystemText systemWriter =
            new IonWriterSystemText(system.getSystemSymbolTable(),
                                    out, options);

        return new IonWriterUserText(catalog, system, systemWriter, options,
                                     getImports());
    }

    @Override
    public final IonWriter build(OutputStream out)
    {
        IonCatalog catalog = getCatalog();
        if (catalog == null) catalog = new SimpleCatalog();

        // TODO We shouldn't need a system here
        IonSystem system =
            IonSystemBuilder.standard().withCatalog(catalog).build();

        _Private_TextOptions options = effectiveOptions();

        IonWriterSystemText systemWriter =
            new IonWriterSystemText(system.getSystemSymbolTable(),
                                    out, options);

        return new IonWriterUserText(catalog, system, systemWriter, options,
                                     getImports());
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
        public void setImports(SymbolTable... imports)
        {
            mutationFailure();
        }
    }
}
