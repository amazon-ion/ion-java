// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.system.IonSystemBuilder;
import com.amazon.ion.util._Private_FastAppendable;

import java.io.OutputStream;

import static com.amazon.ion.impl._Private_Utils.initialSymtab;

/**
 * Contains configuration for Ion 1.0 text writers.
 * NOT FOR APPLICATION USE!
 */
public class _Private_IonTextWriterBuilder_1_0 extends _Private_IonTextWriterBuilder<_Private_IonTextWriterBuilder_1_0> {

    public static _Private_IonTextWriterBuilder_1_0 standard()
    {
        return new _Private_IonTextWriterBuilder_1_0.Mutable();
    }

    public static final _Private_IonTextWriterBuilder_1_0 STANDARD = standard().immutable();

    private _Private_IonTextWriterBuilder_1_0()
    {
        super();
    }

    private _Private_IonTextWriterBuilder_1_0(_Private_IonTextWriterBuilder_1_0 that)
    {
        super(that);
    }

    @Override
    public final _Private_IonTextWriterBuilder_1_0 copy()
    {
        return new Mutable(this);
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

        return new IonWriterUser(catalog, system, systemWriter, initialSymtab, !_allow_invalid_sids);
    }


    @Override
    public final IonWriter build(Appendable out)
    {
        _Private_IonTextWriterBuilder_1_0 b = fillDefaults();

        _Private_FastAppendable fast = new AppendableFastAppendable(out);

        return b.build(fast);
    }


    @Override
    public final IonWriter build(OutputStream out)
    {
        _Private_IonTextWriterBuilder_1_0 b = fillDefaults();

        _Private_FastAppendable fast = new OutputStreamFastAppendable(out);

        return b.build(fast);
    }

    //=========================================================================

    private static final class Mutable
        extends _Private_IonTextWriterBuilder_1_0
    {
        private Mutable() { }

        private Mutable(_Private_IonTextWriterBuilder_1_0 that)
        {
            super(that);
        }

        @Override
        public _Private_IonTextWriterBuilder_1_0 immutable()
        {
            return new _Private_IonTextWriterBuilder_1_0(this);
        }

        @Override
        public _Private_IonTextWriterBuilder_1_0 mutable()
        {
            return this;
        }

        @Override
        protected void mutationCheck()
        {
        }
    }
}
