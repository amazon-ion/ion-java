/*
 * Copyright (c) 2007-2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.util;

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
import com.amazon.ion.IonValue;
import com.amazon.ion.TtTimestamp;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;


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
        public boolean decimalAsFloat;
        public boolean skipAnnotations;
        public boolean sexpAsList;
        public boolean stringAsJson;
        public boolean symbolAsString;
        public boolean timestampAsString;
        public boolean timestampAsMillis;
        public boolean untypedNulls;
//      public boolean skipSystemValues = true;


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
     * Sets whether this printer ignores (<em>i.e.</em>, doesn't print)
     * annotations.  By default, this is <code>false</code>.
     */
    public synchronized boolean getSkipAnnotations()
    {
        return myOptions.skipAnnotations;
    }

    public synchronized void setSkipAnnotations(boolean ignore)
    {
        myOptions.skipAnnotations = ignore;
    }


    /**
     * Sets whether this printer renders blobs as Base64 strings.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintBlobAsString()
    {
        return myOptions.blobAsString;
    }

    public synchronized void setPrintBlobAsString(boolean blobAsString)
    {
        myOptions.blobAsString = blobAsString;
    }


    /**
     * Sets whether this printer renders clobs as ASCII strings.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintClobAsString()
    {
        return myOptions.clobAsString;
    }

    public synchronized void setPrintClobAsString(boolean clobAsString)
    {
        myOptions.clobAsString = clobAsString;
    }


    /**
     * Sets whether this printer renders decimals as floats, thus using 'e'
     * notation for all real values.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintDecimalAsFloat()
    {
        return myOptions.decimalAsFloat;
    }

    public synchronized void setPrintDecimalAsFloat(boolean decimalAsFloat)
    {
        myOptions.decimalAsFloat = decimalAsFloat;
    }


    /**
     * Sets whether this printer renders sexps as lists.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintSexpAsList()
    {
        return myOptions.sexpAsList;
    }

    public synchronized void setPrintSexpAsList(boolean sexpAsList)
    {
        myOptions.sexpAsList = sexpAsList;
    }


    /**
     * Sets whether this printer renders strings using JSON escapes.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintStringlAsJson()
    {
        return myOptions.stringAsJson;
    }

    public synchronized void setPrintStringAsJson(boolean stringAsJson)
    {
        myOptions.stringAsJson = stringAsJson;
    }


    /**
     * Sets whether this printer renders symbols as strings.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintSymbolAsString()
    {
        return myOptions.symbolAsString;
    }

    public synchronized void setPrintSymbolAsString(boolean symbolAsString)
    {
        myOptions.symbolAsString = symbolAsString;
    }

    /**
     * Sets whether this printer renders timestamps as millisecond values.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintTimestampAsMillis()
    {
        return myOptions.timestampAsMillis;
    }

    public synchronized void setPrintTimestampAsMillis(boolean timestampAsMillis)
    {
        myOptions.timestampAsMillis = timestampAsMillis;
    }

    /**
     * Sets whether this printer renders timestamps as strings.
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintTimestampAsString()
    {
        return myOptions.timestampAsString;
    }

    public synchronized void setPrintTimestampAsString(boolean timestampAsString)
    {
        myOptions.timestampAsString = timestampAsString;
    }


    /**
     * Sets whether this printer renders all null values as <code>null</code>
     * (<em>i.e.</em>, the same as an {@link IonNull}).
     * By default, this is <code>false</code>.
     */
    public synchronized boolean getPrintUntypedNulls()
    {
        return myOptions.untypedNulls;
    }

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
     *   <li>{@link Options#decimalAsFloat}</li> is {@code true}
     *   <li>{@link Options#skipAnnotations}</li> is {@code true}
     *   <li>{@link Options#sexpAsList}</li> is {@code true}
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
        myOptions.decimalAsFloat    = true;
        myOptions.skipAnnotations   = true;
        myOptions.sexpAsList        = true;
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

        _print(value, makeVisitor(options, out));
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
                String[] anns = value.getTypeAnnotations();
                if (anns != null)
                {
                    for (String ann : anns) {
                        writeSymbol(ann);
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


        public void writeSymbol(String text) throws IOException
        {
            if (myOptions.symbolAsString)
            {
                writeString(text);
            }
            else if (Text.symbolNeedsQuoting(text, myQuoteOperators))
            {
                Text.printQuotedSymbol(myOut, text);
            }
            else
            {
                myOut.append(text);
            }
        }


        public void writeString(String text) throws IOException
        {
            if (myOptions.stringAsJson)
            {
                Text.printJsonString(myOut, text);
            }
            else
            {
                Text.printString(myOut, text);
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
                value.appendBase64(myOut);
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
                            Text.printJsonCodePoint(myOut, c);
                        }
                    }
                    else
                    {
                        while ((c = byteStream.read()) != -1)
                        {
                            Text.printStringCodePoint(myOut, c);
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
            boolean hitOne = false;
            for (IonValue child : value)
            {
                if (hitOne)
                {
                    myOut.append(' ');
                }
                hitOne = true;

                writeChild(child, false);
            }
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
                BigDecimal decimal = value.bigDecimalValue();
                BigInteger unscaled = decimal.unscaledValue();

                myOut.append(unscaled.toString());
                myOut.append(myOptions.decimalAsFloat ? 'e' : 'd');
                myOut.append(Integer.toString(-decimal.scale()));
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
                // FIXME broken for over-long values
                myOut.append(Long.toString(value.longValue()));
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

                    writeSymbol(child.getFieldName());
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

            if (value.isNullValue())
            {
                writeNull("symbol");
            }
            else
            {
                writeSymbol(value.stringValue());
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
                TtTimestamp ts = value.timestampValue();

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
            Text.printJsonString(myOut, text);
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
