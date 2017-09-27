package software.amazon.ion.impl;

import java.io.IOException;
import java.util.Iterator;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;
import software.amazon.ion.SymbolToken;

/**
 * Base class for {@link SymbolTable} wrapper. Subclasses can add and/or override methods as needed
 * <p>
 * Delegates all {@link SymbolTable} methods to internal {@link SymbolTable} instance
 * </p>
 */
abstract class BaseSymbolTableWrapper implements SymbolTable
{
    private final SymbolTable delegate;

    protected BaseSymbolTableWrapper(SymbolTable delegate)
    {
        this.delegate = delegate;
    }

    protected final SymbolTable getDelegate()
    {
        return delegate;
    }

    public String getName()
    {
        return delegate.getName();
    }

    public int getVersion()
    {
        return delegate.getVersion();
    }

    public boolean isLocalTable()
    {
        return delegate.isLocalTable();
    }

    public boolean isSharedTable()
    {
        return delegate.isSharedTable();
    }

    public boolean isSubstitute()
    {
        return delegate.isSubstitute();
    }

    public boolean isSystemTable()
    {
        return delegate.isSystemTable();
    }

    public boolean isReadOnly()
    {
        return delegate.isReadOnly();
    }

    public void makeReadOnly()
    {
        delegate.makeReadOnly();
    }

    public SymbolTable getSystemSymbolTable()
    {
        return delegate.getSystemSymbolTable();
    }

    public String getIonVersionId()
    {
        return delegate.getIonVersionId();
    }

    public SymbolTable[] getImportedTables()
    {
        return delegate.getImportedTables();
    }

    public int getImportedMaxId()
    {
        return delegate.getImportedMaxId();
    }

    public int getMaxId()
    {
        return delegate.getMaxId();
    }

    public SymbolToken intern(String text)
    {
        return delegate.intern(text);
    }

    public SymbolToken find(String text)
    {
        return delegate.find(text);
    }

    public int findSymbol(String name)
    {
        return delegate.findSymbol(name);
    }

    public String findKnownSymbol(int id)
    {
        return delegate.findKnownSymbol(id);
    }

    public Iterator<String> iterateDeclaredSymbolNames()
    {
        return delegate.iterateDeclaredSymbolNames();
    }

    public void writeTo(IonWriter writer) throws IOException
    {
        delegate.writeTo(writer);
    }
}

