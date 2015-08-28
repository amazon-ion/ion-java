package com.amazon.ion.impl.bin;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

import com.amazon.ion.IonMutableCatalog;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolTable;
import com.amazon.ion.impl.bin.IonManagedBinaryWriterBuilder.AllocatorMode;
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
}
