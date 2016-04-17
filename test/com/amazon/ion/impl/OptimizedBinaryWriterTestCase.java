// Copyright (c) 2013-2014 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.impl;

import static com.amazon.ion.Symtabs.makeLocalSymtab;
import static com.amazon.ion.SystemSymbols.ION_SYMBOL_TABLE;
import static java.lang.reflect.Proxy.newProxyInstance;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.junit.Injected.Inject;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonSystemBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * Base test case for {@link IonWriter#writeValue(IonReader)}, with and without
 * copy optimization enabled.
 *
 * @see IonSystemBuilder#withStreamCopyOptimized(boolean)
 */
public class OptimizedBinaryWriterTestCase
    extends IonTestCase
{
    @Inject("copySpeed")
    public static final StreamCopySpeed[] COPY_SPEEDS =
        StreamCopySpeed.values();

    /**
     * Denotes whether the
     * {@link PrivateByteTransferReader#transferCurrentValue(IonWriterSystemBinary)}
     * has been called after an {@link IonWriter#writeValue(IonReader)}.
     */
    private boolean isTransferCurrentValueInvoked = false;

    protected static final String importFred1 = ION_SYMBOL_TABLE +
        "::{imports:[{name:\"fred\",version:1,max_id:2}]}";

    protected static final String importFred2 = ION_SYMBOL_TABLE +
        "::{imports:[{name:\"fred\",version:2,max_id:4}]}";

    protected ByteArrayOutputStream myOutputStream;
    protected IonWriter iw;
    protected IonReader ir;

    protected void checkWriterStreamCopyOptimized(IonWriter writer)
    {
        if (isStreamCopyOptimized())
        {
            assertTrue("IonWriter should be instance of _Private_IonWriter",
                       writer instanceof PrivateIonWriter);
            PrivateIonWriter privateWriter = (PrivateIonWriter) writer;
            assertTrue("IonWriter should be stream copy optimized",
                       privateWriter.isStreamCopyOptimized());
        }
    }

    protected IonWriter makeWriter(SymbolTable... imports)
        throws Exception
    {
        myOutputStream = new ByteArrayOutputStream();
        iw = system().newBinaryWriter(myOutputStream, imports);
        checkWriterStreamCopyOptimized(iw);

        return iw;
    }

    protected IonWriter makeWriterWithLocalSymtab(String... localSymbols)
    {
        myOutputStream = new ByteArrayOutputStream();

        SymbolTable localSymtab = makeLocalSymtab(system(), localSymbols);

        iw = IonBinaryWriterBuilder.standard()
                .withInitialSymbolTable(localSymtab)
                .withStreamCopyOptimized(isStreamCopyOptimized())
                .build(myOutputStream);
        checkWriterStreamCopyOptimized(iw);

        return iw;
    }

    protected byte[] outputByteArray()
        throws Exception
    {
        iw.close();

        byte[] bytes = myOutputStream.toByteArray();
        return bytes;
    }

    private class TransferCurrentValueWatchingReader
        implements PrivateByteTransferReader
    {
        private final PrivateByteTransferReader myDelegate;

        TransferCurrentValueWatchingReader(PrivateByteTransferReader byteTransferReader)
        {
            myDelegate = byteTransferReader;
        }

        public void transferCurrentValue(PrivateByteTransferSink sink)
            throws IOException
        {
            OptimizedBinaryWriterTestCase.this.isTransferCurrentValueInvoked = true;
            myDelegate.transferCurrentValue(sink);
        }
    }

    /**
     * Obtains a dynamic proxy of {@link IonReader} over the passed in byte[],
     * with an invocation handler hook over {@link PrivateByteTransferReader} facet,
     * so as to verify whether the transferCurrentValue() method is actually
     * being called.
     *
     * @see TransferCurrentValueWatchingReader
     */
    protected IonReader makeReaderProxy(byte[] bytes)
    {
        final IonReader reader = system().newReader(bytes);

        InvocationHandler handler = new InvocationHandler()
        {
            public Object invoke(Object proxy, Method method, Object[] args)
                throws Throwable
            {
                if (method.getName().equals("asFacet") &&
                    args.length == 1 &&
                    args[0] == PrivateByteTransferReader.class)
                {
                    PrivateByteTransferReader transferReader =
                        (PrivateByteTransferReader) method.invoke(reader, args);

                    if (transferReader == null)
                        return null;

                    return new TransferCurrentValueWatchingReader(transferReader);
                }

                return method.invoke(reader, args);
            }
        };

        return (IonReader) newProxyInstance(reader.getClass().getClassLoader(),
                                            new Class[] { IonReader.class },
                                            handler);
    }

    /**
     * Calls {@code iw.writeValue(ir)} and checks whether copy optimization
     * has been made through the {@link #isTransferCurrentValueInvoked} boolean.
     *
     * @param expectedTransferInvoked
     *          the expected value of {@link #isTransferCurrentValueInvoked}
     *          after {@link IonWriter#writeValue(IonReader)} is called.
     */
    protected void checkWriteValue(boolean expectedTransferInvoked)
        throws Exception
    {
        // Reset flag before calling IonWriter.writeValue
        isTransferCurrentValueInvoked = false;

        // TODO amznlabs/ion-java#16 - Currently, doesn't copy annotations or field names,
        //      so we always expect no transfer of raw bytes
        if (ir.isInStruct() || ir.getTypeAnnotationSymbols().length > 0)
        {
            expectedTransferInvoked = false;
        }

        iw.writeValue(ir); // method in test

        assertEquals(expectedTransferInvoked, isTransferCurrentValueInvoked);
    }

}
