package software.amazon.ion.impl;

import java.util.Arrays;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.SymbolTable;
import static software.amazon.ion.SymbolTable.UNKNOWN_SYMBOL_ID;
import software.amazon.ion.SymbolToken;

public class LocalSymbolTableImportAdapterTest
{
    private static SymbolTableMockBuilder symbolTableMockBuilder()
    {
        return new SymbolTableMockBuilder();
    }

    @Test
    public void testIsReadOnlyByDefault()
        throws Exception
    {
        LocalSymbolTableImportAdapter subject = new LocalSymbolTableImportAdapter( symbolTableMockBuilder().build());

        assertEquals(true, subject.isReadOnly());
    }

    @Test
    public void testNoSystemSymbolTable()
        throws Exception
    {
        LocalSymbolTableImportAdapter subject = new LocalSymbolTableImportAdapter( symbolTableMockBuilder().build());

        assertEquals(null, subject.getSystemSymbolTable());
    }

    @Test
    public void testImportedTables()
        throws Exception
    {
        SymbolTable imported1 = mock(SymbolTable.class, "imported1");
        SymbolTable imported2 = mock(SymbolTable.class, "imported2");

        SymbolTable delegate =  symbolTableMockBuilder()
            .setImportedTables(imported1, imported2)
            .build();

        LocalSymbolTableImportAdapter subject = new LocalSymbolTableImportAdapter(delegate);

        assertEquals(2, subject.getImportedTables().length);
        assertEquals(imported1, subject.getImportedTables()[0]);
        assertEquals(imported2, subject.getImportedTables()[1]);
    }

    @Test
    public void testMaxIdDelegateWithSystemTable()
        throws Exception
    {
        SymbolTable systemTable =  symbolTableMockBuilder()
            .setMaxId(4)
            .build();

        SymbolTable delegate =  symbolTableMockBuilder()
            .setMaxId(10)
            .setSystemTable(systemTable)
            .build();

        when(delegate.getSystemSymbolTable()).thenReturn(systemTable);

        LocalSymbolTableImportAdapter subject = new LocalSymbolTableImportAdapter(delegate);

        assertEquals(6, subject.getMaxId());
    }

    @Test
    public void testMaxIdDelegateWithoutSystemTable()
        throws Exception
    {
        SymbolTable delegate =  symbolTableMockBuilder()
            .setMaxId(10)
            .build();

        LocalSymbolTableImportAdapter subject = new LocalSymbolTableImportAdapter(delegate);

        assertEquals(10, subject.getMaxId());
    }

    @Test
    public void testImportedMaxIdDelegateWithSystemTable()
        throws Exception
    {
        SymbolTable systemTable =  symbolTableMockBuilder()
            .setMaxId(4)
            .build();

        SymbolTable delegate =  symbolTableMockBuilder()
            .setMaxId(10)
            .setImportedMaxId(4)
            .setSystemTable(systemTable)
            .build();

        when(delegate.getSystemSymbolTable()).thenReturn(systemTable);

        LocalSymbolTableImportAdapter subject = new LocalSymbolTableImportAdapter(delegate);

        // ignores symbols imported from system table so imported are 4 - 4 = 0
        assertEquals(0, subject.getImportedMaxId());
    }

    @Test
    public void testImportedMaxIdDelegateWithoutSystemTable()
        throws Exception
    {
        SymbolTable delegate =  symbolTableMockBuilder()
            .setMaxId(10)
            .setImportedMaxId(4)
            .build();

        LocalSymbolTableImportAdapter subject = new LocalSymbolTableImportAdapter(delegate);

        assertEquals(4, subject.getImportedMaxId());
    }

    @Test(expected = ReadOnlyValueException.class)
    public void testIntern()
        throws Exception
    {
        LocalSymbolTableImportAdapter subject = new LocalSymbolTableImportAdapter( symbolTableMockBuilder().build());

        subject.intern("");
    }

    @Test
    public void testFind()
        throws Exception
    {
        SymbolTable imported =  symbolTableMockBuilder()
            .setSymbols("onImport")
            .setMaxId(1)
            .build();

        SymbolTable delegate =  symbolTableMockBuilder()
            .setImportedMaxId(1)
            .setMaxId(2)
            .setImportedTables(imported)
            .setSymbols("onDelegate")
            .build();

        LocalSymbolTableImportAdapter subject = new LocalSymbolTableImportAdapter(delegate);

        SymbolToken onDelegateSymbol = subject.find("onDelegate");
        assertEquals("onDelegate", onDelegateSymbol.getText());
        assertEquals(2, onDelegateSymbol.getSid());

        SymbolToken onImportSymbol = subject.find("onImport");
        assertEquals("onImport", onImportSymbol.getText());
        assertEquals(1, onImportSymbol.getSid());

        SymbolToken unknown = subject.find("unknown");
        assertNull(unknown);
    }


    @Test
    public void testFindSymbol()
        throws Exception
    {
        SymbolTable imported = symbolTableMockBuilder()
            .setMockName("imported")
            .setSymbols("onImport")
            .setMaxId(1)
            .build();

        SymbolTable delegate =  symbolTableMockBuilder()
            .setMockName("delegate")
            .setImportedMaxId(1)
            .setMaxId(2)
            .setImportedTables(imported)
            .setSymbols("onDelegate")
            .build();

        LocalSymbolTableImportAdapter subject = new LocalSymbolTableImportAdapter(delegate);

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
        SymbolTable imported =  symbolTableMockBuilder()
            .setMockName("imported")
            .setSymbols("onImport")
            .setMaxId(1)
            .build();

        SymbolTable delegate =  symbolTableMockBuilder()
            .setMockName("delegate")
            .setImportedMaxId(1)
            .setMaxId(2)
            .setImportedTables(imported)
            .setSymbols("onDelegate")
            .build();

        LocalSymbolTableImportAdapter subject = new LocalSymbolTableImportAdapter(delegate);

        String onDelegate = subject.findKnownSymbol(2);
        assertEquals("onDelegate", onDelegate);

        String onImport = subject.findKnownSymbol(1);
        assertEquals("onImport", onImport);

        String unknown = subject.findKnownSymbol(99);
        assertNull(unknown);
    }

    private static class SymbolTableMockBuilder
    {
        private int maxId = 0;
        private int importedMaxId = 0;
        private String[] symbols = new String[0];
        private SymbolTable[] importedTables = new SymbolTable[0];
        private String mockName = null;

        private SymbolTable systemTable = null;

        public SymbolTable build()
        {
            SymbolTable mock = mock(SymbolTable.class, mockName);

            when(mock.getMaxId()).thenReturn(maxId);
            when(mock.getImportedMaxId()).thenReturn(importedMaxId);
            when(mock.getImportedTables()).thenReturn(importedTables);
            when(mock.getSystemSymbolTable()).thenReturn(systemTable);

            when(mock.iterateDeclaredSymbolNames()).thenReturn(Arrays.asList(symbols).iterator());
            for(int i = 0; i < symbols.length; i++)
            {
                String text = symbols[i];
                int sid = importedMaxId + i + 1;

                when(mock.find(text)).thenReturn(new SymbolTokenImpl(text, sid));
                when(mock.findKnownSymbol(sid)).thenReturn(text);
            }

            return mock;
        }

        public SymbolTableMockBuilder setMockName(String mockName)
        {
            this.mockName = mockName;

            return this;
        }

        public SymbolTableMockBuilder setSystemTable(SymbolTable systemTable)
        {
            this.systemTable = systemTable;

            return this;
        }

        public SymbolTableMockBuilder setMaxId(final int maxId)
        {
            this.maxId = maxId;

            return this;
        }

        public SymbolTableMockBuilder setImportedMaxId(final int importedMaxId)
        {
            this.importedMaxId = importedMaxId;

            return this;
        }

        public SymbolTableMockBuilder setSymbols(final String... symbols)
        {
            this.symbols = symbols;

            return this;
        }

        public SymbolTableMockBuilder setImportedTables(final SymbolTable... importedTables)
        {
            this.importedTables = importedTables;

            return this;
        }
    }
}
