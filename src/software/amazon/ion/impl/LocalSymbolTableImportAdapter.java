package software.amazon.ion.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import software.amazon.ion.ReadOnlyValueException;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;

/**
 * Adapter for {@link LocalSymbolTable} to make it importable by ignoring any potentially imported
 * system table.
 * <p>
 * <strong>Important:</strong> All adapted instances are read only
 * </p>
 */
final class LocalSymbolTableImportAdapter
    extends BaseSymbolTableWrapper
{
    private final List<SymbolTable> imports;

    private final int systemSymbolCount;
    private final int maxImportedId;
    private final int maxId;
    private final int numberOfLocalSymbols;

    // Lazy, access only through getters
    private Map<String, Integer> localTextToSidMap;
    private String[] sidToLocalText;
    private SymbolTable[] importedTables;

    static LocalSymbolTableImportAdapter of(final LocalSymbolTable delegate)
    {
        /*
         * There is a hard boundary between the data regions from the imported and derived table. Example:
         *
         * LST A that contains symbol A1, LST B is created importing LST A and after B is created symbol A2 is added
         * to LST A. LST B should have symbol A1, through the import, but it should not have symbol A2 as it was included
         * after LST B creation
         *
         * To maintain this boundary we make a read only copy for mutable LSTs. Copying is not necessary for read only LSTs
         * as no extra items can be added into them
         */
        final LocalSymbolTable readOnlyDelegate;
        if(delegate.isReadOnly())
        {
            readOnlyDelegate = delegate;
        }
        else {
            readOnlyDelegate = delegate.makeCopy();
            readOnlyDelegate.makeReadOnly();
        }

        return new LocalSymbolTableImportAdapter(readOnlyDelegate);
    }

    private LocalSymbolTableImportAdapter(final LocalSymbolTable delegate)
    {
        super(delegate);

        imports = createImportList(delegate);

        systemSymbolCount = countSystemSymbols(getDelegate());
        maxImportedId = getDelegate().getImportedMaxId() - systemSymbolCount;
        maxId = getDelegate().getMaxId() - systemSymbolCount;
        numberOfLocalSymbols = maxId - maxImportedId;
    }

    @Override
    public boolean isReadOnly()
    {
        return true;
    }

    @Override
    public void makeReadOnly()
    {
        // read only by default
    }

    @Override
    public SymbolTable getSystemSymbolTable()
    {
        return null;
    }

    @Override
    public SymbolTable[] getImportedTables()
    {
        if (importedTables == null)
        {
            importedTables = imports.toArray(new SymbolTable[imports.size()]);
        }

        return importedTables;
    }

    @Override
    public int getImportedMaxId()
    {
        return maxImportedId;
    }

    @Override
    public int getMaxId()
    {
        return maxId;
    }

    @Override
    public SymbolToken intern(String text)
    {
        throw new ReadOnlyValueException(this.getClass());
    }

    @Override
    public SymbolToken find(String text)
    {
        for (SymbolTable importedTable : imports)
        {
            SymbolToken symbolToken = importedTable.find(text);

            if (symbolToken != null)
            {
                return symbolToken;
            }
        }

        Integer sid = getLocalTextToSidMap().get(text);
        if (sid != null)
        {
            return new SymbolTokenImpl(text, sid);
        }

        return null;
    }

    @Override
    public int findSymbol(String name)
    {
        SymbolToken token = find(name);
        return (token == null ? UNKNOWN_SYMBOL_ID : token.getSid());
    }

    @Override
    public String findKnownSymbol(int id)
    {
        if (id < 0)
        {
            throw new IllegalArgumentException("symbol IDs must be greater than 0");
        }

        if (id > getMaxId()) // not in this table or imports
        {
            return null;
        }

        // look into imports
        if (id <= maxImportedId)
        {
            for (SymbolTable importedSymbolTable : imports)
            {
                String text = importedSymbolTable.findKnownSymbol(id);
                if (text != null)
                {
                    return text;
                }
            }
        }

        // not in imports get from local
        int index = id - maxImportedId - 1;
        return getSidToLocalText()[index];
    }

    private List<SymbolTable> createImportList(LocalSymbolTable delegate)
    {
        SymbolTable[] asArray = delegate.getImportedTables();
        List<SymbolTable> imports = new ArrayList<SymbolTable>(asArray.length);
        for (SymbolTable s : asArray)
        {
            if (!s.isSystemTable())
            {
                imports.add(s);
            }
        }

        return imports;
    }

    private int countSystemSymbols(SymbolTable symbolTable)
    {
        SymbolTable systemSymbolTable = symbolTable.getSystemSymbolTable();

        if(systemSymbolTable != null)
        {
            return systemSymbolTable.getMaxId();
        }

        return 0;
    }

    private Map<String, Integer> getLocalTextToSidMap()
    {
        if (localTextToSidMap == null)
        {
            localTextToSidMap = new HashMap<String, Integer>(numberOfLocalSymbols);

            for (Iterator<String> it = iterateDeclaredSymbolNames(); it.hasNext(); )
            {
                SymbolToken symbolToken = getDelegate().find(it.next());
                String text = symbolToken.getText();

                // have to adjust SID to ignore potential system tables
                int relativeSid = symbolToken.getSid() - systemSymbolCount;
                localTextToSidMap.put(text, relativeSid);
            }
        }

        return localTextToSidMap;
    }

    private String[] getSidToLocalText()
    {
        if (sidToLocalText == null)
        {
            sidToLocalText = new String[numberOfLocalSymbols];

            for (Iterator<String> it = getDelegate().iterateDeclaredSymbolNames(); it.hasNext(); )
            {
                SymbolToken symbolToken = getDelegate().find(it.next());
                String text = symbolToken.getText();
                int relativeSid = symbolToken.getSid() - getDelegate().getImportedMaxId();

                sidToLocalText[relativeSid - 1] = text;
            }
        }

        return sidToLocalText;
    }
}
