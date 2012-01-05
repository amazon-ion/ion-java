// Copyright (c) 2007-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import static com.amazon.ion.SystemSymbols.IMPORTS;
import static com.amazon.ion.SystemSymbols.ION_1_0_SID;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static com.amazon.ion.SystemSymbols.SYMBOLS;

import com.amazon.ion.Decimal;
import com.amazon.ion.InternedSymbol;
import com.amazon.ion.IonBlob;
import com.amazon.ion.IonBool;
import com.amazon.ion.IonClob;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonList;
import com.amazon.ion.IonNull;
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
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl._Private_IonTextWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder.InitialIvmHandling;
import com.amazon.ion.util.IonTextUtils.SymbolVariant;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Iterator;


/**
 * Renders {@link IonValue}s to text.
 * <p>
 * By default, output is in a compact format with minimal whitespace.
 * For example:
 * <pre>
 *    annot::{f1:["hello","goodbye"],'another field':long::0}
 * </pre>
 * The format can be tuned through various properties on the Printer instance,
 * as well as through the {@link Printer.Options} structure.
 * <p>
 * While printers are inexpensive to create, their configuration facilities
 * make them useful as shared resources. Printers are safe for use from
 * multiple threads.  Changes to configuration settings (<em>e.g.</em>,
 * {@link #setJsonMode()}) do not affect concurrently-running calls to
 * {@link #print}.
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
     *   <li>{@link Options#blobAsString}</li> is {@code true}
     *   <li>{@link Options#clobAsString}</li> is {@code true}
     *   <li>{@link Options#datagramAsList}</li> is {@code true}
     *   <li>{@link Options#decimalAsFloat}</li> is {@code true}
     *   <li>{@link Options#sexpAsList}</li> is {@code true}
     *   <li>{@link Options#skipAnnotations}</li> is {@code true}
     *   <li>{@link Options#skipSystemValues}</li> is {@code true}
     *   <li>{@link Options#stringAsJson}</li> is {@code true}
     *   <li>{@link Options#symbolAsString}</li> is {@code true}
     *   <li>{@link Options#timestampAsString}</li> is {@code false}
     *   <li>{@link Options#timestampAsMillis}</li> is {@code true}
     *   <li>{@link Options#untypedNulls}</li> is {@code true}
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
            // TODO ION-10
            // Bridge to the configurable test writer. This is here for
            // testing purposes. It *almost* works except for some wonkyness
            // at the IVM/symtab level where the two paths behave differently.

            boolean dg = value instanceof IonDatagram;

            _Private_IonTextWriterBuilder o = _Private_IonTextWriterBuilder.standard();
            o.setCharset(IonTextWriterBuilder.ASCII);
            if (!dg)
            {
                o.setInitialIvmHandling(InitialIvmHandling.SUPPRESS);
            }
            o._filter_symbol_tables = options.skipSystemValues;

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
            value.writeTo(writer);
            writer.finish();
        }
    }

    /**
     *  Convert an IonValue to a legal JSON string:
     *
     *  - discard annotations
     *  - always quote symbols
     *  - sexp's?
     *  @deprecated use {@link #setJsonMode()}.
     */

    @Deprecated
    public void printJson(IonValue value, Appendable out)
        throws IOException
    {
        // Copy the options so visitor won't see changes made while printing.
        Options options;
        synchronized (this)  // So we don't clone in the midst of changes
        {
            options = myOptions.clone();
        }

        _print(value, new JsonPrinterVisitor(options, out));
    }

    private void _print(IonValue value, PrinterVisitor pv)
        throws IOException
    {
        try
        {
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


        //---------------------------------------------------------------------

        public PrinterVisitor(Options options, Appendable out)
        {
            myOptions = options;
            myOut = out;
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
                InternedSymbol[] anns = value.getTypeAnnotationSymbols();
                if (anns != null)
                {
                    for (InternedSymbol ann : anns) {
                        String text = ann.getText();
                        if (text == null) {
                            myOut.append('$');
                            myOut.append(Integer.toString(ann.getId()));
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

        public void writeSymbolToken(InternedSymbol sym) throws IOException
        {
            String text = sym.getText();
            if (text != null)
            {
                writeSymbol(text);
            }
            else
            {
                int sid = sym.getId();
                if (sid < 1)
                {
                    throw new IllegalArgumentException("Bad SID " + sid);
                }

                text = "$" + sym.getId();
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

            // If we're skipping system values we don't need to simplify them.
            final boolean simplify_system_values =
                myOptions.simplifySystemValues && ! myOptions.skipSystemValues;

            SymbolTable previous_symbols = null;

            while (i.hasNext())
            {
                IonValue child = i.next();

                if (simplify_system_values)
                {
                    SymbolTable new_symbols = child.getSymbolTable();
                    child = simplify(child, previous_symbols, new_symbols);
                    previous_symbols = new_symbols;
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
                                        SymbolTable previous_symbols,
                                        SymbolTable new_symbols)
        {
            IonType t = child.getType();
            switch (t) {
            case STRUCT:
                if (child.hasTypeAnnotation(ION_SYMBOL_TABLE)) {
                    // TODO What keeps us from having this struct as first thing
                    // in the datagram?  It could be manually constructed.
                    assert(previous_symbols != null && new_symbols != null);

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

                // shortcut zero cases
                if (real == 0.0)
                {
                    // XXX use the raw bits to avoid boxing and distinguish +/-0e0
                    long bits = Double.doubleToLongBits(real);
                    if (bits == 0L)
                    {
                        // positive zero
                        myOut.append("0e0");
                    }
                    else
                    {
                        // negative zero
                        myOut.append("-0e0");
                    }
                }
                else if (Double.isNaN(real))
                {
                    myOut.append("nan");
                }
                else if (real == Double.POSITIVE_INFINITY)
                {
                    myOut.append("+inf");
                }
                else if (real == Double.NEGATIVE_INFINITY)
                {
                    myOut.append("-inf");
                }
                else
                {
                    BigDecimal decimal = value.bigDecimalValue();
                    BigInteger unscaled = decimal.unscaledValue();

                    myOut.append(unscaled.toString());
                    myOut.append('e');
                    myOut.append(Integer.toString(-decimal.scale()));
                }
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

                    InternedSymbol sym = child.getFieldNameSymbol();
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

            InternedSymbol is = value.symbolValue();
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
        } // PrinterVisitor.visit(IonTimestamp)
    }

    /**
     * <b>This class is unsupported and will be removed!</b>
     * @deprecated
     */
    @Deprecated
    public final static class JsonPrinterVisitor
    extends PrinterVisitor
    {
        public JsonPrinterVisitor(Options options, Appendable out)
        {
            super(options, out);
        }

        @Override
        public void writeAnnotations(IonValue value)
        throws IOException
        {}

        @Override
        public void writeSymbol(String text)
        throws IOException
        {
            IonTextUtils.printJsonString(myOut, text);
        }

        public void writeFloat(BigDecimal value)
        throws IOException
        {
            BigInteger unscaled = value.unscaledValue();

            myOut.append(unscaled.toString());
            myOut.append('e');
            myOut.append(Integer.toString(-value.scale()));
        }

        @Override
        public void visit(IonTimestamp value) throws IOException
        {
            if (value.isNullValue()) {
                myOut.append("null");
            } else {
                myOut.append(Long.toString(value.getMillis()));
            }
        }

        @Override
        public void visit(IonList value) throws IOException, Exception
        {
            if (value.isNullValue()) {
                myOut.append("null");
            } else {
                super.visit(value);
            }
        }

        @Override
        public void visit(IonStruct value) throws IOException, Exception
        {
            if (value.isNullValue()) {
                myOut.append("null");
            } else {
                super.visit(value);
            }
        }

        @Override
        public void visit(IonString value) throws IOException
        {
            if (value.isNullValue()) {
                myOut.append("null");
            } else {
                writeString(value.stringValue());
            }
        }

        @Override
        public void visit(IonDecimal value) throws IOException
        {
            if (value.isNullValue()) {
                myOut.append("null");
            } else {
                writeFloat(value.bigDecimalValue());
            }
        }

        @Override
        public void visit(IonFloat value) throws IOException
        {
            if (value.isNullValue()) {
                myOut.append("null");
            } else {
                writeFloat(value.bigDecimalValue());
            }
        }

        @Override
        public void visit(IonSexp value) throws IOException, Exception
        {
            if (value.isNullValue())
            {
                myOut.append("null");
            }
            else
            {
                myOut.append('[');

                boolean hitOne = false;
                for (IonValue child : value)
                {
                    if (hitOne)
                    {
                        myOut.append(',');
                    }
                    hitOne = true;
                    writeChild(child, false);
                }
                myOut.append(']');
            }
        }
    }
}
