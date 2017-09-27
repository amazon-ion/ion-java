package software.amazon.ion.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonWriter;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.SymbolTable;
import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import software.amazon.ion.SymbolToken;

public class LocalSymbolTableImportAdapterTest extends BaseSymbolTableWrapperTest
{
    @Test
    public void testIsReadOnlyByDefault()
    {
        LocalSymbolTable lst = lstBuilder().build();
        assertEquals(false, lst.isReadOnly());

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(lst);

        assertEquals(true, subject.isReadOnly());
    }

    @Override
    public void testGetSystemSymbolTable()
    {
        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(lstBuilder().build());

        assertEquals(null, subject.getSystemSymbolTable());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testImportedTables()
    {
        LocalSymbolTable imported1 = lstBuilder().build();
        LocalSymbolTable imported2 = lstBuilder().build();

        lstBuilder().setImportedTables(imported1, imported2).build();
    }

    @Test
    public void testMaxIdNoImports()
    {
        LocalSymbolTable delegate = lstBuilder()
            .setSymbols("one", "two")
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        assertEquals(2, subject.getMaxId());
    }

    @Override
    public void testGetMaxId()
    {
        SymbolTable imported = lstBuilder().setSymbols("1", "2", "3").build();

        LocalSymbolTable delegate = lstBuilder()
            .setSymbols("one", "two")
            .setImportedTables(imported)
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        assertEquals(5, subject.getMaxId());
    }

    @Test
    public void testImportedMaxIdNoImports()
    {
        LocalSymbolTable delegate = lstBuilder()
            .setSymbols("one", "two")
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        assertEquals(0, subject.getImportedMaxId());
    }

    @Override
    public void testGetImportedMaxId()
    {
        SymbolTable imported = lstBuilder().setSymbols("1", "2", "3").build();

        LocalSymbolTable delegate = lstBuilder()
            .setSymbols("one", "two")
            .setImportedTables(imported)
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        assertEquals(3, subject.getImportedMaxId());
    }

    @Test(expected = ReadOnlyValueException.class)
    @Override
    public void testIntern()
    {
        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(lstBuilder().build());

        subject.intern("");
    }

    @Override
    public void testFind()
    {
        SymbolTable imported = lstBuilder().setSymbols("onImport").build();

        LocalSymbolTable delegate = lstBuilder()
            .setImportedTables(imported)
            .setSymbols("onDelegate")
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        SymbolToken onDelegateSymbol = subject.find("onDelegate");
        assertNotNull(onDelegateSymbol);
        assertEquals("onDelegate", onDelegateSymbol.getText());
        assertEquals(2, onDelegateSymbol.getSid());

        SymbolToken onImportSymbol = subject.find("onImport");
        assertNotNull(onImportSymbol);
        assertEquals("onImport", onImportSymbol.getText());
        assertEquals(1, onImportSymbol.getSid());

        SymbolToken unknown = subject.find("unknown");
        assertNull(unknown);
    }

    @Override
    protected BaseSymbolTableWrapper getSubject()
    {
        return LocalSymbolTableImportAdapter.of(lstBuilder().build());
    }

    @Override
    public void testMakeReadOnly()
    {
        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(lstBuilder().build());
        subject.makeReadOnly();
        assertEquals(true, subject.isReadOnly());

        try
        {
            subject.intern("asd");
            Assert.fail("could modify read only");
        } catch (ReadOnlyValueException ignored)
        {

        }
    }

    @Override
    public void testFindSymbol()
    {
        SymbolTable imported = lstBuilder().setSymbols("onImport").build();

        LocalSymbolTable delegate = lstBuilder()
            .setImportedTables(imported)
            .setSymbols("onDelegate")
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        int onDelegateSid = subject.findSymbol("onDelegate");
        assertEquals(2, onDelegateSid);

        int onImportSid = subject.findSymbol("onImport");
        assertEquals(1, onImportSid);

        int unknown = subject.findSymbol("unknown");
        assertEquals(UNKNOWN_SYMBOL_ID, unknown);
    }

    @Override
    public void testFindKnownSymbol()
    {
        SymbolTable imported = lstBuilder().setSymbols("onImport").build();

        LocalSymbolTable delegate = lstBuilder()
            .setImportedTables(imported)
            .setSymbols("onDelegate")
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        String onDelegate = subject.findKnownSymbol(2);
        assertEquals("onDelegate", onDelegate);

        String onImport = subject.findKnownSymbol(1);
        assertEquals("onImport", onImport);

        String unknown = subject.findKnownSymbol(99);
        assertNull(unknown);
    }

    @Override
    public void testWriteTo() throws IOException
    {
        LocalSymbolTable delegate = lstBuilder().setSymbols("mySymbol").build();
        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        StringBuilder out = new StringBuilder();

        IonWriter ionWriter = system().newTextWriter(out);
        subject.writeTo(ionWriter);
        ionWriter.close();

        assertEquals(out.toString(), "$ion_symbol_table::{symbols:[\"mySymbol\"]}");
    }

    @Test
    public void testWriteToBinary() throws IOException
    {
        LocalSymbolTable delegate = lstBuilder().setSymbols("mySymbol").build();
        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        IonWriter ionWriter = system().newBinaryWriter(out);
        subject.writeTo(ionWriter);
        system().newInt(1).writeTo(ionWriter);
        ionWriter.close();

        IonDatagram datagram = loader().load(out.toByteArray());
        SymbolTable symbolTable = datagram.get(0).getSymbolTable();

        assertEquals(10, symbolTable.getMaxId());
        assertEquals(10, symbolTable.findSymbol("mySymbol"));
    }

    private LocalSymbolTableBuilder lstBuilder()
    {
        return new LocalSymbolTableBuilder(system());
    }

    private static class LocalSymbolTableBuilder
    {
        private final PrivateIonSystem system;
        private String[] symbols = new String[0];
        private SymbolTable[] importedTables = new SymbolTable[0];

        private LocalSymbolTableBuilder(PrivateIonSystem system)
        {
            this.system = system;
        }

        public LocalSymbolTable build()
        {
            return LocalSymbolTable.makeNewLocalSymbolTable(
                system,
                system.getSystemSymbolTable(),
                Arrays.asList(symbols),
                importedTables
            );
        }

        public LocalSymbolTableBuilder setSymbols(final String... symbols)
        {
            this.symbols = symbols;

            return this;
        }

        public LocalSymbolTableBuilder setImportedTables(final SymbolTable... importedTables)
        {
            this.importedTables = importedTables;

            return this;
        }
    }
}
