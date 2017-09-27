package software.amazon.ion.impl;

import java.util.Arrays;
import org.junit.Test;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.SymbolTable;
import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import software.amazon.ion.SymbolToken;

public class LocalSymbolTableImportAdapterTest extends IonTestCase
{
    @Test
    public void testIsReadOnlyByDefault()
        throws Exception
    {
        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(lstBuilder().build());

        assertEquals(true, subject.isReadOnly());
    }

    @Test
    public void testNoSystemSymbolTable()
        throws Exception
    {
        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(lstBuilder().build());

        assertEquals(null, subject.getSystemSymbolTable());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testImportedTables()
        throws Exception
    {
        LocalSymbolTable imported1 = lstBuilder().build();
        LocalSymbolTable imported2 = lstBuilder().build();

        lstBuilder().setImportedTables(imported1, imported2).build();
    }

    @Test
    public void testMaxIdNoImports()
        throws Exception
    {
        LocalSymbolTable delegate = lstBuilder()
            .setSymbols("one", "two")
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        assertEquals(2, subject.getMaxId());
    }

    @Test
    public void testMaxIdWithImports()
        throws Exception
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
        throws Exception
    {
        LocalSymbolTable delegate = lstBuilder()
            .setSymbols("one", "two")
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        assertEquals(0, subject.getImportedMaxId());
    }

    @Test
    public void testImportedMaxIdWithImports()
        throws Exception
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
    public void testIntern()
        throws Exception
    {
        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(lstBuilder().build());

        subject.intern("");
    }

    @Test
    public void testFind()
        throws Exception
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

    private LocalSymbolTableBuilder lstBuilder()
    {
        return new LocalSymbolTableBuilder(system());
    }

    @Test
    public void testFindSymbol()
        throws Exception
    {
        SymbolTable imported = lstBuilder().setSymbols("onImport").build();

        LocalSymbolTable delegate =  lstBuilder()
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

    @Test
    public void testFindKnownSymbol()
        throws Exception
    {
        SymbolTable imported = lstBuilder().setSymbols("onImport").build();

        LocalSymbolTable delegate =  lstBuilder()
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
            LocalSymbolTable localSymbolTable = LocalSymbolTable.makeNewLocalSymbolTable(
                system,
                system.getSystemSymbolTable(),
                Arrays.asList(symbols),
                importedTables
            );

            localSymbolTable.makeReadOnly();

            return localSymbolTable;
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
