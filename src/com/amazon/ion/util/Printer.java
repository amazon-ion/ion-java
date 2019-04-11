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

package com.amazon.ion.util;

import static com.amazon.ion.SystemSymbols.IMPORTS;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.SYMBOLS;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonBlob;
import com.amazon.ion.IonBool;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonNull;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonString;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl._Private_IonSymbol;
import com.amazon.ion.impl._Private_IonSystem;
import com.amazon.ion.impl._Private_IonTextWriterBuilder;
import com.amazon.ion.impl._Private_IonValue;
import com.amazon.ion.impl._Private_IonValue.SymbolTableProvider;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder.LstMinimizing;
import com.amazon.ion.system.IonWriterBuilder.IvmMinimizing;
import com.amazon.ion.util.IonTextUtils.SymbolVariant;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Iterator;


/**
 * Renders {@link IonValue}s to text.
 * <p>
 * By default, output is in a compact format with minimal whitespace.
 * For example:
 *<pre>
 *    annot::{f1:["hello","goodbye"],'another field':long::0}
 *</pre>
 * The format can be tuned through various properties on the Printer instance,
 * as well as through the {@link Printer.Options} structure.
 * <p>
 * <b>Instances of this class are safe for use by multiple threads.</b>
 * <p>
 * While printers are inexpensive to create, their configuration facilities
 * make them useful as shared resources. Changes to configuration settings
 * (<em>e.g.</em>, {@link #setJsonMode()}) do not affect concurrently-running
 * calls to {@link #print}.
 *
 * @see IonWriter
 * @see IonTextWriterBuilder
 */
public class Printer
{
    public class Options
        implements Cloneable
    {
        public boolean blobAsString;
        public boolean clobAsString;
        public boolean datagramAsList;
        public boolean decimalAsFloat;
        public boolean sexpAsList;
        public boolean skipAnnotations;
        public boolean skipSystemValues;
        public boolean simplifySystemValues;
        public boolean stringAsJson;
        public boolean symbolAsString;
        public boolean timestampAsString;
        public boolean timestampAsMillis;
        public boolean untypedNulls;


        @Override
        public Options clone()
        {
            try
            {
                return (Options) super.clone();
            }
            catch (CloneNotSupportedException e)
            {
                throw new InternalError();
            }
        }
    }


    protected Options myOptions = new Options();

    public Printer()
    {
        myOptions = new Options();
    }

    public Printer(Options options)
    {
        myOptions = options.clone();
    }


    //=========================================================================
    // Options

    /* Potential printing options:
     *
     * - Render all times with a specific offset (what about unknowns?)
     * - Render all times with long offset (no Z)
     * - control over decimal-point placement in float and decimal.
     *   (render 12.00 or 1200d-2)
     */


    /**
     * Indicates whether this printer skips (<em>i.e.</em>, doesn't print)
     * system IDs and local symbol tables.
     * By default, this property is <code>false</code>.
     */
    public synchronized boolean getSkipSystemValues()
    {
        return myOptions.skipSystemValues;
    }

    /**
     * Sets whether this printer skips (<em>i.e.</em>, doesn't print)
     * system IDs and local symbol tables.
     * By default, this property is <code>false</code>.
     */
    public synchronized void setSkipSystemValues(boolean skip)
    {
        myOptions.skipSystemValues = skip;
    }


    /**
     * Indicates whether this printer skips (<em>i.e.</em>, doesn't print)
     * annotations.
     * By default, this property is <code>false</code>.
     */
    public synchronized boolean getSkipAnnotations()
    {
        return myOptions.skipAnnotations;
    }

    /**
     * Sets whether this printer skips (<em>i.e.</em>, doesn't print)
     * annotations.
     * By default, this property is <code>false</code>.
     */
    public synchronized void setSkipAnnotations(boolean skip)
    {
        myOptions.skipAnnotations = skip;
    }


    /**
     * Indicates whether this printer renders blobs as Base64 strings.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintBlobAsString()
    {
        return myOptions.blobAsString;
    }

    /**
     * Sets whether this printer renders blobs as Base64 strings.
     * By default, this is <code>false</code>.
     */
    public synchronized void setPrintBlobAsString(boolean blobAsString)
    {
        myOptions.blobAsString = blobAsString;
    }


    /**
     * Indicates whether this printer renders clobs as ASCII strings.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintClobAsString()
    {
        return myOptions.clobAsString;
    }

    /**
     * Sets whether this printer renders clobs as ASCII strings.
     * By default, this is <code>false</code>.
     */
    public synchronized void setPrintClobAsString(boolean clobAsString)
    {
        myOptions.clobAsString = clobAsString;
    }


    /**
     * Indicates whether this printer renders datagrams as lists.
     * By default, this property is <code>false</code>.
     */
    public synchronized boolean getPrintDatagramAsList()
    {
        return myOptions.datagramAsList;
    }

    /**
     * Sets whether this printer renders datagrams as lists.
     * By default, this property is <code>false</code>.
     */
    public synchronized void setPrintDatagramAsList(boolean datagramAsList)
    {
        myOptions.datagramAsList = datagramAsList;
    }


    /**
     * Indicates whether this printer renders decimals as floats, thus using 'e'
     * notation for all real values.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintDecimalAsFloat()
    {
        return myOptions.decimalAsFloat;
    }

    /**
     * Sets whether this printer renders decimals as floats, thus using 'e'
     * notation for all real values.
     * By default, this is <code>false</code>.
     */
    public synchronized void setPrintDecimalAsFloat(boolean decimalAsFloat)
    {
        myOptions.decimalAsFloat = decimalAsFloat;
    }


    /**
     * Indicates whether this printer renders sexps as lists.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintSexpAsList()
    {
        return myOptions.sexpAsList;
    }

    /**
     * Sets whether this printer renders sexps as lists.
     * By default, this is <code>false</code>.
     */
    public synchronized void setPrintSexpAsList(boolean sexpAsList)
    {
        myOptions.sexpAsList = sexpAsList;
    }


    /**
     * Indicates whether this printer renders strings using JSON escapes.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintStringAsJson()
    {
        return myOptions.stringAsJson;
    }

    /**
     * Sets whether this printer renders strings using JSON escapes.
     * By default, this is <code>false</code>.
     */
    public synchronized void setPrintStringAsJson(boolean stringAsJson)
    {
        myOptions.stringAsJson = stringAsJson;
    }


    /**
     * Indicates whether this printer renders symbols as strings.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintSymbolAsString()
    {
        return myOptions.symbolAsString;
    }

    /**
     * Sets whether this printer renders symbols as strings.
     * By default, this is <code>false</code>.
     */
    public synchronized void setPrintSymbolAsString(boolean symbolAsString)
    {
        myOptions.symbolAsString = symbolAsString;
    }


    /**
     * Indicates whether this printer renders timestamps as millisecond values.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintTimestampAsMillis()
    {
        return myOptions.timestampAsMillis;
    }

    /**
     * Sets whether this printer renders timestamps as millisecond values.
     * By default, this is <code>false</code>.
     */
    public synchronized void setPrintTimestampAsMillis(boolean timestampAsMillis)
    {
        myOptions.timestampAsMillis = timestampAsMillis;
    }


    /**
     * Indicates whether this printer renders timestamps as strings.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintTimestampAsString()
    {
        return myOptions.timestampAsString;
    }

    /**
     * Sets whether this printer renders timestamps as strings.
     * By default, this is <code>false</code>.
     */
    public synchronized void setPrintTimestampAsString(boolean timestampAsString)
    {
        myOptions.timestampAsString = timestampAsString;
    }


    /**
     * Indicates whether this printer renders all null values as {@code null}
     * (<em>i.e.</em>, the same as an {@link IonNull}).
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintUntypedNulls()
    {
        return myOptions.untypedNulls;
    }

    /**
     * Sets whether this printer renders all null values as {@code null}
     * (<em>i.e.</em>, the same as an {@link IonNull}).
     * By default, this is <code>false</code>.
     */
    public synchronized void setPrintUntypedNulls(boolean untypedNulls)
    {
        myOptions.untypedNulls = untypedNulls;
    }


    /**
     * Configures this printer's options to render legal JSON text.
     * The following options are modified so that:
     * <ul>
     *   <li>{@link Options#blobAsString} is {@code true}</li>
     *   <li>{@link Options#clobAsString} is {@code true}</li>
     *   <li>{@link Options#datagramAsList} is {@code true}</li>
     *   <li>{@link Options#decimalAsFloat} is {@code true}</li>
     *   <li>{@link Options#sexpAsList} is {@code true}</li>
     *   <li>{@link Options#skipAnnotations} is {@code true}</li>
     *   <li>{@link Options#skipSystemValues} is {@code true}</li>
     *   <li>{@link Options#stringAsJson} is {@code true}</li>
     *   <li>{@link Options#symbolAsString} is {@code true}</li>
     *   <li>{@link Options#timestampAsString} is {@code false}</li>
     *   <li>{@link Options#timestampAsMillis} is {@code true}</li>
     *   <li>{@link Options#untypedNulls} is {@code true}</li>
     * </ul>
     * All other options are left as is.
     */
    public synchronized void setJsonMode()
    {
        myOptions.blobAsString      = true;
        myOptions.clobAsString      = true;
        myOptions.datagramAsList    = true;
        myOptions.decimalAsFloat    = true;
        myOptions.sexpAsList        = true;
        myOptions.skipAnnotations   = true;
        myOptions.skipSystemValues  = true;
        myOptions.stringAsJson      = true;
        myOptions.symbolAsString    = true;
        myOptions.timestampAsString = false;
        myOptions.timestampAsMillis = true;
        myOptions.untypedNulls      = true;
    }


    //=========================================================================
    // Print methods


    public void print(IonValue value, Appendable out)
        throws IOException
    {
        // Copy the options so visitor won't see changes made while printing.
        Options options;
        synchronized (this)  // So we don't clone in the midst of changes
        {
            options = myOptions.clone();
        }

        if (true)
        {
            _print(value, makeVisitor(options, out));
        }
        else
        {
            // Bridge to the configurable text writer. This is here for
            // testing purposes. It *almost* works except for printing
            // datagram as list.

            boolean dg = value instanceof IonDatagram;

            _Private_IonTextWriterBuilder o =
                _Private_IonTextWriterBuilder.standard();
            o.setCharset(IonTextWriterBuilder.ASCII);
            if (dg)
            {
                if (options.skipSystemValues)
                {
                    o.withMinimalSystemData();
                }
                else if (options.simplifySystemValues) {

                    o.setIvmMinimizing(IvmMinimizing.DISTANT);
                    o.setLstMinimizing(LstMinimizing.LOCALS);
                }
            }

            o._blob_as_string      = options.blobAsString;
            o._clob_as_string      = options.clobAsString;
            o._decimal_as_float    = options.decimalAsFloat;
            // TODO datagram as list
            o._sexp_as_list        = options.sexpAsList;
            o._skip_annotations    = options.skipAnnotations;
            o._string_as_json      = options.stringAsJson;
            o._symbol_as_string    = options.symbolAsString;
            o._timestamp_as_millis = options.timestampAsMillis;
            o._timestamp_as_string = options.timestampAsString;
            o._untyped_nulls       = options.untypedNulls;

            IonWriter writer = o.build(out);
            // TODO doesn't work for datagram since it skips system values
            // value.writeTo(writer);
            _Private_IonSystem system = (_Private_IonSystem) value.getSystem();
            IonReader reader = system.newSystemReader(value);
            writer.writeValues(reader);
            writer.finish();
        }
    }

    private void _print(IonValue value, PrinterVisitor pv)
        throws IOException
    {
        try
        {
            if (! (value instanceof IonDatagram))
            {
                pv.setSymbolTableProvider(new BasicSymbolTableProvider(value.getSymbolTable()));
            }
            value.accept(pv);
        }
        catch (IOException e)
        {
            throw e;
        }
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            // Shouldn't happen.
            throw new RuntimeException(e);
        }
    }


    /**
     * Subclasses can override this if they wish to construct a specialization
     * of the {@link PrinterVisitor}.
     *
     * @param options is a fresh copy of the Printer's options instance,
     * not <code>null</code>.
     * @param out is not <code>null</code>.
     * @return the visitor to invoke for printing.
     */
    protected PrinterVisitor makeVisitor(Options options, Appendable out)
    {
        return new PrinterVisitor(options, out);
    }


    private static class BasicSymbolTableProvider
        implements SymbolTableProvider
    {
        private final SymbolTable symbolTable;

        public BasicSymbolTableProvider(SymbolTable symbolTable)
        {
            this.symbolTable = symbolTable;
        }

        public SymbolTable getSymbolTable()
        {
            return symbolTable;
        }

    }

    //=========================================================================
    // Print methods

    public static class PrinterVisitor
        extends AbstractValueVisitor
    {

        final protected Options    myOptions;
        final protected Appendable myOut;

        /**
         * Should we quote operators at the current level of the hierarchy?
         * As we recurse down into containers, this value is pushed on the
         * stack by {@link #writeChild(IonValue, boolean)}.
         */
        private boolean    myQuoteOperators = true;

        private SymbolTableProvider mySymbolTableProvider = null;


        //---------------------------------------------------------------------

        public PrinterVisitor(Options options, Appendable out)
        {
            myOptions = options;
            myOut = out;
        }

        void setSymbolTableProvider(SymbolTableProvider symbolTableProvider)
        {
            mySymbolTableProvider = symbolTableProvider;
        }

        /**
         * Recurse down into a container, we push the current value of
         * {@link #myQuoteOperators} onto the stack and replace it with
         * the given value.
         *
         * @param value
         * @param quoteOperators replaces the current value of
         * {@link #myQuoteOperators} during the recursive visitation.
         * @throws Exception propagated from visitation of <code>value</code>.
         * @throws NullPointerException if <code>value</code> is null.
         */
        protected void writeChild(IonValue value, boolean quoteOperators)
            throws Exception
        {
            boolean oldQuoteOperators = myQuoteOperators;
            myQuoteOperators = quoteOperators;
            value.accept(this);
            myQuoteOperators = oldQuoteOperators;
        }


        public void writeAnnotations(IonValue value) throws IOException
        {
            if (! myOptions.skipAnnotations)
            {
                SymbolToken[] anns = ((_Private_IonValue)value).getTypeAnnotationSymbols(mySymbolTableProvider);
                if (anns != null)
                {
                    for (SymbolToken ann : anns) {
                        String text = ann.getText();
                        if (text == null) {
                            myOut.append('$');
                            myOut.append(Integer.toString(ann.getSid()));
                        }
                        else {
                            IonTextUtils.printSymbol(myOut, text);
                        }
                        myOut.append("::");
                    }
                }
            }
        }

        public void writeNull(String type) throws IOException
        {
            if (myOptions.untypedNulls)
            {
                myOut.append("null");
            }
            else
            {
                myOut.append("null.");
                myOut.append(type);
            }
        }


        public void writeSequenceContent(IonSequence value,
                                         boolean quoteOperators,
                                         char open,
                                         char separator,
                                         char close)
            throws IOException, Exception
        {
            myOut.append(open);

            boolean hitOne = false;
            for (IonValue child : value)
            {
                if (hitOne)
                {
                    myOut.append(separator);
                }
                hitOne = true;

                writeChild(child, quoteOperators);
            }

            myOut.append(close);
        }

        public void writeSymbolToken(SymbolToken sym) throws IOException
        {
            String text = sym.getText();
            if (text != null)
            {
                writeSymbol(text);
            }
            else
            {
                int sid = sym.getSid();
                if (sid < 0)
                {
                    throw new IllegalArgumentException("Bad SID " + sid);
                }

                text = "$" + sym.getSid();
                if (myOptions.symbolAsString)
                {
                    writeString(text);
                }
                else
                {
                    myOut.append(text);  // SID literal is never quoted
                }
            }
        }

        public void writeSymbol(String text) throws IOException
        {
            if (myOptions.symbolAsString)
            {
                writeString(text);
            }
            else
            {
                SymbolVariant variant = IonTextUtils.symbolVariant(text);
                switch (variant)
                {
                    case IDENTIFIER:
                        myOut.append(text);
                        break;
                    case OPERATOR:
                        if (! myQuoteOperators)
                        {
                            myOut.append(text);
                            break;
                        }
                        // else fall through...
                    case QUOTED:
                        IonTextUtils.printQuotedSymbol(myOut, text);
                        break;
                }
            }
        }


        /**
         * @param text may be null
         */
        public void writeString(String text) throws IOException
        {
            if (myOptions.stringAsJson)
            {
                IonTextUtils.printJsonString(myOut, text);
            }
            else
            {
                IonTextUtils.printString(myOut, text);
            }
        }


        //---------------------------------------------------------------------
        // AbstractValueVisitor overrides

        @Override
        protected void defaultVisit(IonValue value)
        {
            String message = "cannot print " + value.getClass().getName();
            throw new UnsupportedOperationException(message);
        }

        @Override
        public void visit(IonBlob value) throws IOException
        {
            writeAnnotations(value);

            if (value.isNullValue())
            {
                writeNull("blob");
            }
            else
            {
                myOut.append(myOptions.blobAsString ? "\"" : "{{");
                value.printBase64(myOut);
                myOut.append(myOptions.blobAsString ? "\"" : "}}");
            }
        }

        @Override
        public void visit(IonBool value)
            throws IOException
        {
            writeAnnotations(value);

            if (value.isNullValue())
            {
                writeNull("bool");
            }
            else
            {
                myOut.append(value.booleanValue() ? "true" : "false");
            }
        }

        @Override
        public void visit(IonClob value) throws IOException
        {
            writeAnnotations(value);

            if (value.isNullValue())
            {
                writeNull("clob");
            }
            else
            {
                if (! myOptions.clobAsString)
                {
                    myOut.append("{{");
                }
                myOut.append('"');

                InputStream byteStream = value.newInputStream();
                try
                {
                    int c;
                    // if-statement hoisted above loop for efficiency
                    if (myOptions.stringAsJson)
                    {
                        while ((c = byteStream.read()) != -1)
                        {
                            IonTextUtils.printJsonCodePoint(myOut, c);
                        }
                    }
                    else
                    {
                        while ((c = byteStream.read()) != -1)
                        {
                            IonTextUtils.printStringCodePoint(myOut, c);
                        }
                    }
                }
                finally
                {
                    byteStream.close();
                }

                myOut.append('"');
                if (! myOptions.clobAsString)
                {
                    myOut.append("}}");
                }
            }
        }

        @Override
        public void visit(IonDatagram value) throws IOException, Exception
        {
            Iterator<IonValue> i = (myOptions.skipSystemValues
                                       ? value.iterator()
                                       : value.systemIterator());

            final boolean asList = myOptions.datagramAsList;
            if (asList)
            {
                myOut.append('[');
            }

            boolean hitOne = false;

            // If we're skipping system values at the iterator level,
            // we don't need to bother trying to simplify them.
            final boolean simplify_system_values =
                myOptions.simplifySystemValues && ! myOptions.skipSystemValues;

            SymbolTable previous_symbols = null;

            while (i.hasNext())
            {
                IonValue child = i.next();
                SymbolTable childSymbolTable = child.getSymbolTable();
                mySymbolTableProvider = new BasicSymbolTableProvider(childSymbolTable); //children of datagrams are top-level values
                if (simplify_system_values)
                {
                    child = simplify(child, previous_symbols);
                    previous_symbols = childSymbolTable;
                }

                if (child != null)
                {
                    if (hitOne)
                    {
                        myOut.append(asList  ?  ','  :  ' ');
                    }
                    writeChild(child, true);
                    hitOne = true; // we've only "hit one" if we wrote it
                }
            }

            if (asList)
            {
                myOut.append(']');
            }
        }

        private final IonValue simplify(IonValue child,
                                        SymbolTable previous_symbols)
        {
            IonType t = child.getType();
            switch (t) {
            case STRUCT:
                if (child.hasTypeAnnotation(ION_SYMBOL_TABLE)) {
                    if (symbol_table_struct_has_imports(child))
                    {
                        return ((IonStruct)child).cloneAndRemove(SYMBOLS);
                    }
                    return null;
                }
                // fall through to default (print the value)
                break;
            case SYMBOL:
                if (((IonSymbol)child).getSymbolId() == ION_1_0_SID) {
                    if (previous_symbols != null && previous_symbols.isSystemTable()) {
                        return null;
                    }
                    // fall through to default (print the value)
                }
                // fall through to default (print the value)
                break;
            default:
                break;
            }
            return child;
        }

        static final private boolean symbol_table_struct_has_imports(IonValue child) {
            IonStruct struct = (IonStruct)child;
            IonValue imports = struct.get(IMPORTS);
            if (imports instanceof IonList) {
                return ((IonList)imports).size() != 0;
            }
            return false;
        }

        @Override
        public void visit(IonDecimal value) throws IOException
        {
            writeAnnotations(value);

            if (value.isNullValue())
            {
                writeNull("decimal");
            }
            else
            {
                Decimal decimal = value.decimalValue();
                BigInteger unscaled = decimal.unscaledValue();

                int signum = decimal.signum();
                if (signum < 0)
                {
                    myOut.append('-');
                    unscaled = unscaled.negate();
                }
                else if (signum == 0 && decimal.isNegativeZero())
                {
                    // for the various forms of negative zero we have to
                    // write the sign ourselves, since neither BigInteger
                    // nor BigDecimal recognize negative zero, but Ion does.
                    myOut.append('-');
                }


                final String unscaledText = unscaled.toString();
                final int significantDigits = unscaledText.length();

                final int scale = decimal.scale();
                final int exponent = -scale;

                if (myOptions.decimalAsFloat)
                {
                    myOut.append(unscaledText);
                    myOut.append('e');
                    myOut.append(Integer.toString(exponent));
                }
                else if (exponent == 0)
                {
                    myOut.append(unscaledText);
                    myOut.append('.');
                }
                else if (0 < scale)
                {
                    int wholeDigits;
                    int remainingScale;
                    if (significantDigits > scale)
                    {
                        wholeDigits = significantDigits - scale;
                        remainingScale = 0;
                    }
                    else
                    {
                        wholeDigits = 1;
                        remainingScale = scale - significantDigits + 1;
                    }

                    myOut.append(unscaledText, 0, wholeDigits);
                    if (wholeDigits < significantDigits)
                    {
                        myOut.append('.');
                        myOut.append(unscaledText, wholeDigits,
                                     significantDigits);
                    }

                    if (remainingScale != 0)
                    {
                        myOut.append("d-");
                        myOut.append(Integer.toString(remainingScale));
                    }
                }
                else // (exponent > 0)
                {
                    // We cannot move the decimal point to the right, adding
                    // rightmost zeros, because that would alter the precision.
                    myOut.append(unscaledText);
                    myOut.append('d');
                    myOut.append(Integer.toString(exponent));
                }
            }
        }

        @Override
        public void visit(IonFloat value) throws IOException
        {
            writeAnnotations(value);

            if (value.isNullValue())
            {
                writeNull("float");
            }
            else
            {
                double real = value.doubleValue();
                IonTextUtils.printFloat(myOut, real);
            }
        }

        @Override
        public void visit(IonInt value) throws IOException
        {
            writeAnnotations(value);

            if (value.isNullValue())
            {
                writeNull("int");
            }
            else
            {
                myOut.append(value.bigIntegerValue().toString(10));
            }
        }

        @Override
        public void visit(IonList value) throws IOException, Exception
        {
            writeAnnotations(value);

            if (value.isNullValue())
            {
                writeNull("list");
            }
            else
            {
                writeSequenceContent(value, true, '[', ',', ']');
            }
        }

        @Override
        public void visit(IonNull value) throws IOException
        {
            writeAnnotations(value);
            myOut.append("null");
        }


        @Override
        public void visit(IonSexp value) throws IOException, Exception
        {
            writeAnnotations(value);

            if (value.isNullValue())
            {
                writeNull("sexp");
            }
            else if (myOptions.sexpAsList)
            {
                writeSequenceContent(value, true, '[', ',', ']');
            }
            else
            {
                writeSequenceContent(value, false, '(', ' ', ')');
            }
        }


        @Override
        public void visit(IonString value) throws IOException
        {
            writeAnnotations(value);

            if (value.isNullValue())
            {
                writeNull("string");
            }
            else
            {
                writeString(value.stringValue());
            }
        }


        @Override
        public void visit(IonStruct value) throws IOException, Exception
        {
            writeAnnotations(value);

            if (value.isNullValue())
            {
                writeNull("struct");
            }
            else
            {
                myOut.append('{');

                boolean hitOne = false;
                for (IonValue child : value)
                {
                    if (hitOne)
                    {
                        myOut.append(',');
                    }
                    hitOne = true;

                    SymbolToken sym = ((_Private_IonValue)child).getFieldNameSymbol(mySymbolTableProvider);
                    writeSymbolToken(sym);
                    myOut.append(':');
                    writeChild(child, true);
                }
                myOut.append('}');
            }
        }


        @Override
        public void visit(IonSymbol value) throws IOException
        {
            writeAnnotations(value);

            SymbolToken is = ((_Private_IonSymbol)value).symbolValue(mySymbolTableProvider);
            if (is == null)
            {
                writeNull("symbol");
            }
            else
            {
                writeSymbolToken(is);
            }
        }


        @Override
        public void visit(IonTimestamp value) throws IOException
        {
            writeAnnotations(value);

            if (value.isNullValue())
            {
                writeNull("timestamp");
            }
            else if (myOptions.timestampAsMillis)
            {
                myOut.append(Long.toString(value.getMillis()));
            }
            else
            {
                Timestamp ts = value.timestampValue();

                if (myOptions.timestampAsString)
                {
                    myOut.append('"');
                    ts.print(myOut);
                    myOut.append('"');
                }
                else
                {
                    ts.print(myOut);
                }
            }
        }
    }
}
