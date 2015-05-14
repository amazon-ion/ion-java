package com.amazon.ion.impl.bin;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import com.amazon.ion.IonMutableCatalog;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.bin.IonManagedBinaryWriterBuilder.AllocatorMode;
import com.amazon.ion.system.IonTextWriterBuilder;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class IonManagedBinaryWriterTest extends IonRawBinaryWriterTest
{
    private static final List<String> SHARED_SYMBOLS = unmodifiableList(asList(
        "a",
        "b",
        "c"
    ));

    @Override
    protected IonWriter createWriter(final OutputStream out) throws IOException
    {
        final SymbolTable table = system().newSharedSymbolTable("test", 1, SHARED_SYMBOLS.iterator());
        ((IonMutableCatalog) system().getCatalog()).putTable(table);

        final IonWriter writer = IonManagedBinaryWriterBuilder
            .create(AllocatorMode.POOLED)
            .withImports(table)
            .withPreallocationMode(preallocationMode)
            .newWriter(out);

        final SymbolTable locals = writer.getSymbolTable();
        assertEquals(12, locals.getImportedMaxId());

        return writer;
    }

    @Test
    public void testSetStringAnnotations() throws Exception
    {
        writer.setTypeAnnotations("a", "b", "c", "d");
        writer.writeInt(1);
        assertValue("a::b::c::d::1");
    }

    @Test
    public void testAddStringAnnotation() throws Exception
    {
        writer.addTypeAnnotation("a");
        writer.addTypeAnnotation("b");
        writer.writeInt(1);
        assertValue("a::b::1");
    }

    @Test
    public void testUserSymbol() throws Exception
    {
        writer.writeSymbol("hello");
        assertValue("hello");
    }

    @Test
    public void testUserFieldNames() throws Exception
    {
        writer.stepIn(IonType.STRUCT);
        {
            writer.setFieldName("a");
            writer.writeInt(1);

            writer.setFieldName("b");
            writer.writeInt(2);

            writer.setFieldName("c");
            writer.writeInt(3);

            writer.setFieldName("d");
            writer.writeInt(4);

            writer.setFieldName("e");
            writer.writeInt(5);
        }
        writer.stepOut();
        assertValue("{a:1, b:2, c:3, d:4, e:5}");
    }

    private static final IonTextWriterBuilder PRETTY = IonTextWriterBuilder.standard().withPrettyPrinting().withCharsetAscii();

    private static String pretty(final IonValue value)
    {
        final StringBuilder strBuf = new StringBuilder();
        final IonWriter pp = PRETTY.build(strBuf);
        value.writeTo(pp);
        try
        {
            pp.close();
        }
        catch (final IOException e)
        {
            throw new IllegalStateException(e);
        }
        return strBuf.toString();
    }

    /** Simple utility to detect encoding differences from the old and new encoder (with no padding). */
    public static void main(final String[] args) throws Exception
    {

        final IonManagedBinaryWriterBuilder builder =
            IonManagedBinaryWriterBuilder
                .create(AllocatorMode.POOLED)
                .withPaddedLengthPreallocation(0)
                .withUserBlockSize(65536);
        SymbolTable shared = null;

        final String filename = args[0];
        final IonReader reader = system().newReader(new FileInputStream(filename));
        final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try
        {
            int count = 0;
            while (reader.next() != null)
            {
                if (shared == null)
                {
                    final SymbolTable locals = reader.getSymbolTable();
                    shared = system().newSharedSymbolTable("generated", 1, locals.iterateDeclaredSymbolNames());
                    ((IonMutableCatalog) system().getCatalog()).putTable(shared);
                    builder.withImports(shared);
                }

                // do the old writer
                buf.reset();
                {
                    final IonWriter out = system().newBinaryWriter(buf, shared);
                    try
                    {
                        out.writeValue(reader);
                    }
                    finally
                    {
                        out.close();
                    }
                }
                final byte[] oldEncoded = buf.toByteArray();
                buf.reset();

                final IonReader reader2 = system().newReader(oldEncoded);
                try
                {
                    final IonWriter out = builder.newWriter(buf);
                    try
                    {
                        reader2.next();
                        out.writeValue(reader2);
                    }
                    finally
                    {
                        out.close();
                    }
                }
                finally
                {
                    reader2.close();
                }
                final byte[] newEncoded = buf.toByteArray();

                final IonValue oldVal = system().singleValue(oldEncoded);
                try
                {
                    final IonValue newVal = system().singleValue(newEncoded);
                    if (oldEncoded.length < newEncoded.length || !oldVal.equals(newVal))
                    {
                        System.err.println("Content differed (" + count +  "): OLD: " + oldEncoded.length + ", NEW:  " + newEncoded.length);
                        System.err.println(WriteBufferTest.hex(oldEncoded));
                        System.err.println(WriteBufferTest.hex(newEncoded));
                        System.err.println(pretty(oldVal));
                        System.err.println("EQUALS: " + oldVal.equals(newVal));
                        break;
                    }
                }
                catch (final Exception e)
                {
                    System.err.println(WriteBufferTest.hex(oldEncoded));
                    System.err.println(WriteBufferTest.hex(newEncoded));
                    System.err.println(pretty(oldVal));
                    throw e;
                }
                count++;
            }
        } finally
        {
            reader.close();
        }
    }
}
