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
    private final SymbolTable[] imports;
    private final int systemSymbolCount;

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

        imports = createImports(delegate);

        systemSymbolCount = countSystemSymbols(getDelegate());
    }

    @Override
    public SymbolTable getSystemSymbolTable()
    {
        return null;
    }

    @Override
    public SymbolTable[] getImportedTables()
    {
        return imports;
    }

    @Override
    public int getImportedMaxId()
    {
        return getDelegate().getImportedMaxId() - systemSymbolCount;
    }

    @Override
    public int getMaxId()
    {
        return getDelegate().getMaxId() - systemSymbolCount;
    }

    @Override
    public SymbolToken find(String text)
    {
        SymbolToken symbolToken = getDelegate().find(text);
        if(symbolToken != null)
        {
            symbolToken = new SymbolTokenImpl(symbolToken.getText(), symbolToken.getSid() - systemSymbolCount);
        }

        return symbolToken;
    }

    @Override
    public int findSymbol(String name)
    {
        int sid = getDelegate().findSymbol(name);
        if(sid != UNKNOWN_SYMBOL_ID){
            sid = sid - systemSymbolCount;
        }

        return sid;
    }

    @Override
    public String findKnownSymbol(int id)
    {
        return getDelegate().findKnownSymbol(id + systemSymbolCount);
    }

    private SymbolTable[] createImports(LocalSymbolTable delegate)
    {
        SymbolTable[] delegateImports = delegate.getImportedTables();
        SymbolTable[] imports = new SymbolTable[delegateImports.length];

        int j = 0;
        for (SymbolTable delegateImport : delegateImports)
        {
            if (!delegateImport.isSystemTable())
            {
                imports[j++] = delegateImport;
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
}
