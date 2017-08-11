package software.amazon.ion.impl;

import java.util.Arrays;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
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
        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(localSymbolTableBuilder().build());

        assertEquals(true, subject.isReadOnly());
    }

    @Test
    public void testNoSystemSymbolTable()
        throws Exception
    {
        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(localSymbolTableBuilder().build());

        assertEquals(null, subject.getSystemSymbolTable());
    }

    @Test
    public void testImportedTables()
        throws Exception
    {
        SymbolTable imported1 = mockImport();
        SymbolTable imported2 = mockImport();

        LocalSymbolTable delegate = localSymbolTableBuilder()
            .setImportedTables(imported1, imported2)
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        assertEquals(2, subject.getImportedTables().length);
        assertEquals(imported1, subject.getImportedTables()[0]);
        assertEquals(imported2, subject.getImportedTables()[1]);
    }

    @Test
    public void testMaxIdNoImports()
        throws Exception
    {
        LocalSymbolTable delegate = localSymbolTableBuilder()
            .setSymbols("one", "two")
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        assertEquals(2, subject.getMaxId());
    }

    @Test
    public void testMaxIdWithImports()
        throws Exception
    {
        SymbolTable imported = mockImport("1", "2", "3");

        LocalSymbolTable delegate = localSymbolTableBuilder()
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
        LocalSymbolTable delegate = localSymbolTableBuilder()
            .setSymbols("one", "two")
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        assertEquals(0, subject.getImportedMaxId());
    }

    @Test
    public void testImportedMaxIdWithImports()
        throws Exception
    {
        SymbolTable imported = mockImport("1", "2", "3");

        LocalSymbolTable delegate = localSymbolTableBuilder()
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
        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(localSymbolTableBuilder().build());

        subject.intern("");
    }

    @Test
    public void testFind()
        throws Exception
    {
        SymbolTable imported = mockImport("onImport");

        LocalSymbolTable delegate = localSymbolTableBuilder()
            .setImportedTables(imported)
            .setSymbols("onDelegate")
            .build();

        LocalSymbolTableImportAdapter subject = LocalSymbolTableImportAdapter.of(delegate);

        SymbolToken onDelegateSymbol = subject.find("onDelegate");
        assertEquals("onDelegate", onDelegateSymbol.getText());
        assertEquals(2, onDelegateSymbol.getSid());

        SymbolToken onImportSymbol = subject.find("onImport");
        assertEquals("onImport", onImportSymbol.getText());
        assertEquals(1, onImportSymbol.getSid());

        SymbolToken unknown = subject.find("unknown");
        assertNull(unknown);
    }

    private LocalSymbolTableMockBuilder localSymbolTableBuilder()
    {
        return new LocalSymbolTableMockBuilder(system());
    }

    private SymbolTable mockImport(String... symbols)
    {
        SymbolTable mock = mock(SymbolTable.class);
        when(mock.getMaxId()).thenReturn(symbols.length);
        for (int i = 0; i < symbols.length; i++)
        {
            String text = symbols[i];
            int sid = i + 1;

            when(mock.find(text)).thenReturn(new SymbolTokenImpl(text, sid));
            when(mock.findKnownSymbol(sid)).thenReturn(text);
            when(mock.findSymbol(text)).thenReturn(sid);
        }
        return mock;
    }


    @Test
    public void testFindSymbol()
        throws Exception
    {
        SymbolTable imported = mockImport("onImport");

        LocalSymbolTable delegate =  localSymbolTableBuilder()
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
        SymbolTable imported = mockImport("onImport");

        LocalSymbolTable delegate =  localSymbolTableBuilder()
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

    private static class LocalSymbolTableMockBuilder
    {
        private final PrivateIonSystem system;
        private String[] symbols = new String[0];
        private SymbolTable[] importedTables = new SymbolTable[0];

        private LocalSymbolTableMockBuilder(PrivateIonSystem system)
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

        public LocalSymbolTableMockBuilder setSymbols(final String... symbols)
        {
            this.symbols = symbols;

            return this;
        }

        public LocalSymbolTableMockBuilder setImportedTables(final SymbolTable... importedTables)
        {
            this.importedTables = importedTables;

            return this;
        }
    }
}
