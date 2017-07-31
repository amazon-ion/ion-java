package software.amazon.ion.impl;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import org.mockito.runners.MockitoJUnitRunner;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SymbolTable;

@RunWith(MockitoJUnitRunner.class)
public final class BaseSymbolTableWrapperTest
{
    @Mock
    private SymbolTable delegate;

    private BaseSymbolTableWrapper subject;

    @Before
    public void before()
    {
        subject = new BaseSymbolTableWrapper(delegate)
        {
            // empty
        };
    }

    @Test
    public void getName() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.getName();

        verify(delegate).getName();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getVersion() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.getVersion();

        verify(delegate).getVersion();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void isLocalTable() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.isLocalTable();

        verify(delegate).isLocalTable();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void isSharedTable() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.isSharedTable();

        verify(delegate).isSharedTable();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void isSubstitute() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.isSubstitute();

        verify(delegate).isSubstitute();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void isSystemTable() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.isSystemTable();

        verify(delegate).isSystemTable();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void isReadOnly() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.isReadOnly();

        verify(delegate).isReadOnly();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void makeReadOnly() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.makeReadOnly();

        verify(delegate).makeReadOnly();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getSystemSymbolTable() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.getSystemSymbolTable();

        verify(delegate).getSystemSymbolTable();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getIonVersionId() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.getIonVersionId();

        verify(delegate).getIonVersionId();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getImportedTables() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.getImportedTables();

        verify(delegate).getImportedTables();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getImportedMaxId() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.getImportedMaxId();

        verify(delegate).getImportedMaxId();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void getMaxId() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.getMaxId();

        verify(delegate).getMaxId();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void intern() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.intern("");

        verify(delegate).intern("");
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void find() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.find("");

        verify(delegate).find("");
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void findSymbol() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.findSymbol("");

        verify(delegate).findSymbol("");
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void findKnownSymbol() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.findKnownSymbol(1);

        verify(delegate).findKnownSymbol(1);
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void iterateDeclaredSymbolNames() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        subject.iterateDeclaredSymbolNames();

        verify(delegate).iterateDeclaredSymbolNames();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void writeTo() throws Exception
    {
        // verifying no previous interaction
        verifyZeroInteractions(delegate);

        IonWriter writer = mock(IonWriter.class);
        subject.writeTo(writer);

        verify(delegate).writeTo(writer);
        verifyNoMoreInteractions(delegate);
    }
}
