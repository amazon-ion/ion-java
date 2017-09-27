package software.amazon.ion.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.Test;
import software.amazon.ion.IonTestCase;
import software.amazon.ion.SymbolTable;

/**
 * Parent test class for classes that extend {@link BaseSymbolTableWrapper}. Provides simple equality tests for non void
 * methods with no arguments by comparing delegate and subject returns. Other methods and overridden methods need to be
 * handled by the subclasses
 */
public abstract class BaseSymbolTableWrapperTest extends IonTestCase
{
    protected abstract BaseSymbolTableWrapper getSubject();

    @Test
    public void testGetName()
    {
        assertEquals(getDelegate().getName(), getSubject().getName());
    }

    @Test
    public void testGetVersion()
    {
        assertEquals(getDelegate().getVersion(), getSubject().getVersion());
    }

    @Test
    public void testIsLocalTable()
    {
        assertEquals(getDelegate().isLocalTable(), getSubject().isLocalTable());
    }

    @Test
    public void testIsSharedTable()
    {
        assertEquals(getDelegate().isSharedTable(), getSubject().isSharedTable());
    }

    @Test
    public void testIsSubstitute()
    {
        assertEquals(getDelegate().isSubstitute(), getSubject().isSubstitute());
    }

    @Test
    public void testIsSystemTable()
    {
        assertEquals(getDelegate().isSystemTable(), getSubject().isSystemTable());
    }

    @Test
    public void testIsReadOnly()
    {
        assertEquals(getDelegate().isReadOnly(), getSubject().isReadOnly());
    }

    @Test
    public abstract void testMakeReadOnly();

    @Test
    public void testGetSystemSymbolTable()
    {
        assertEquals(getDelegate().getSystemSymbolTable(), getSubject().getSystemSymbolTable());
    }

    @Test
    public void testGetIonVersionId()
    {
        assertEquals(getDelegate().getIonVersionId(), getSubject().getIonVersionId());
    }

    @Test
    public void testGetImportedTables()
    {
        assertEquals(getDelegate().getImportedTables(), getSubject().getImportedTables());
    }

    @Test
    public void testGetImportedMaxId()
    {
        assertEquals(getDelegate().getImportedMaxId(), getSubject().getImportedMaxId());
    }

    @Test
    public void testGetMaxId()
    {
        assertEquals(getDelegate().getMaxId(), getSubject().getMaxId());
    }

    @Test
    public abstract void testIntern();

    @Test
    public abstract void testFind();

    @Test
    public abstract void testFindSymbol();

    @Test
    public abstract void testFindKnownSymbol();

    @Test
    public void testIterateDeclaredSymbolNames()
    {
        Iterator<String> expected = getDelegate().iterateDeclaredSymbolNames();
        Iterator<String> actual = getSubject().iterateDeclaredSymbolNames();

        assertEquals(collect(expected), collect(actual));
    }

    private <T> List<T> collect(Iterator<T> iterator) {
        List<T> list = new ArrayList<T>();

        for(T t = null; iterator.hasNext(); t = iterator.next())
        {
            list.add(t);
        }

        return list;
    }

    @Test
    public abstract void testWriteTo() throws IOException;

    private SymbolTable getDelegate()
    {
        return getSubject().getDelegate();
    }
}
