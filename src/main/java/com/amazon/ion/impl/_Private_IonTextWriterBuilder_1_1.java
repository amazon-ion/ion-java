// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl;

import com.amazon.ion.IonCatalog;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.bin.LengthPrefixStrategy;
import com.amazon.ion.impl.bin.IonManagedWriter_1_1;
import com.amazon.ion.impl.bin.ManagedWriterOptions_1_1;
import com.amazon.ion.impl.bin.SymbolInliningStrategy;
import com.amazon.ion.system.IonTextWriterBuilder_1_1;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Objects;

/**
 * Contains configuration for Ion 1.1 text writers.
 * NOT FOR APPLICATION USE!
 */
public class _Private_IonTextWriterBuilder_1_1
    extends _Private_IonTextWriterBuilder<_Private_IonTextWriterBuilder_1_1> implements IonTextWriterBuilder_1_1 {

    public static _Private_IonTextWriterBuilder_1_1 standard() {
        return new _Private_IonTextWriterBuilder_1_1.Mutable();
    }

    private SymbolInliningStrategy symbolInliningStrategy = SymbolInliningStrategy.ALWAYS_INLINE;

    private _Private_IonTextWriterBuilder_1_1() {
        super();
    }

    private _Private_IonTextWriterBuilder_1_1(_Private_IonTextWriterBuilder_1_1 that) {
        super(that);
        symbolInliningStrategy = that.symbolInliningStrategy;
    }

    @Override
    public SymbolInliningStrategy getSymbolInliningStrategy() {
        return symbolInliningStrategy;
    }

    @Override
    public void setSymbolInliningStrategy(SymbolInliningStrategy symbolInliningStrategy) {
        mutationCheck();
        this.symbolInliningStrategy = Objects.requireNonNull(symbolInliningStrategy);
    }

    @Override
    public IonTextWriterBuilder_1_1 withSymbolInliningStrategy(SymbolInliningStrategy symbolInliningStrategy) {
        _Private_IonTextWriterBuilder_1_1 b = mutable();
        b.setSymbolInliningStrategy(symbolInliningStrategy);
        return b;
    }

    // The following methods are overridden in order to resolve a clashing return type, as they are defined by
    // multiple ancestors (version-agnostic abstract classes and the Ion 1.1 interface).

    @Override
    public _Private_IonTextWriterBuilder_1_1 copy()
    {
        return new _Private_IonTextWriterBuilder_1_1.Mutable(this);
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 immutable()
    {
        return this;
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 mutable()
    {
        return copy();
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withCatalog(IonCatalog catalog) {
        return (_Private_IonTextWriterBuilder_1_1) super.withCatalog(catalog);
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withImports(SymbolTable[] imports) {
        return (_Private_IonTextWriterBuilder_1_1) super.withImports(imports);
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withPrettyPrinting() {
        return (_Private_IonTextWriterBuilder_1_1) super.withPrettyPrinting();
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withJsonDowngrade() {
        return (_Private_IonTextWriterBuilder_1_1) super.withJsonDowngrade();
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withCharset(Charset charset) {
        return (_Private_IonTextWriterBuilder_1_1) super.withCharset(charset);
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withCharsetAscii() {
        return (_Private_IonTextWriterBuilder_1_1) super.withCharsetAscii();
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withMinimalSystemData() {
        return (_Private_IonTextWriterBuilder_1_1) super.withMinimalSystemData();
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withLstMinimizing(LstMinimizing minimizing) {
        return (_Private_IonTextWriterBuilder_1_1) super.withLstMinimizing(minimizing);
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withLongStringThreshold(int threshold) {
        return (_Private_IonTextWriterBuilder_1_1) super.withLongStringThreshold(threshold);
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withNewLineType(NewLineType newLineType) {
        return (_Private_IonTextWriterBuilder_1_1) super.withNewLineType(newLineType);
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withWriteTopLevelValuesOnNewLines(boolean writeTopLevelValuesOnNewLines) {
        return (_Private_IonTextWriterBuilder_1_1) super.withWriteTopLevelValuesOnNewLines(writeTopLevelValuesOnNewLines);
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withMaximumTimestampPrecisionDigits(int maximumTimestampPrecisionDigits) {
        return (_Private_IonTextWriterBuilder_1_1) super.withMaximumTimestampPrecisionDigits(maximumTimestampPrecisionDigits);
    }

    @Override
    public _Private_IonTextWriterBuilder_1_1 withIvmMinimizing(IvmMinimizing minimizing) {
        return (_Private_IonTextWriterBuilder_1_1) super.withIvmMinimizing(minimizing);
    }

    @Override
    public IonWriter build(Appendable out) {
        if (out == null) {
            throw new NullPointerException("Cannot construct a writer with a null Appendable.");
        }
        _Private_IonTextWriterBuilder_1_1 b = fillDefaults();
        ManagedWriterOptions_1_1 options = new ManagedWriterOptions_1_1(
            false,
            true,
            symbolInliningStrategy,
            LengthPrefixStrategy.NEVER_PREFIXED,
            // This could be made configurable.
            ManagedWriterOptions_1_1.EExpressionIdentifierStrategy.BY_NAME
        );
        return IonManagedWriter_1_1.textWriter(out, options, b);
    }

    @Override
    public IonWriter build(OutputStream out) {
        if (out == null) {
            throw new NullPointerException("Cannot construct a writer with a null OutputStream.");
        }

        _Private_IonTextWriterBuilder_1_1 b = fillDefaults();
        ManagedWriterOptions_1_1 options = new ManagedWriterOptions_1_1(
            false,
            true,
            symbolInliningStrategy,
            LengthPrefixStrategy.NEVER_PREFIXED,
            // This could be made configurable.
            ManagedWriterOptions_1_1.EExpressionIdentifierStrategy.BY_NAME
        );
        return IonManagedWriter_1_1.textWriter(out, options, b);
    }

    //=========================================================================

    private static final class Mutable extends _Private_IonTextWriterBuilder_1_1 {
        private Mutable() { }

        private Mutable(_Private_IonTextWriterBuilder_1_1 that) {
            super(that);
        }

        @Override
        public _Private_IonTextWriterBuilder_1_1 immutable() {
            return new _Private_IonTextWriterBuilder_1_1(this);
        }

        @Override
        public _Private_IonTextWriterBuilder_1_1 mutable() {
            return this;
        }

        @Override
        protected void mutationCheck() {
        }
    }
}
