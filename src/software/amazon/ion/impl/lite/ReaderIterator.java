package software.amazon.ion.impl.lite;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import software.amazon.ion.IonException;
import software.amazon.ion.IonReader;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.SymbolTable;

/**
 * An IonReader based IonValue iterator. It closes the reader when there are no
 * more values to iterate over.
 */
public class ReaderIterator
    implements Iterator<IonValue>, Closeable
{
    private final IonReader reader;
    private final IonSystemLite system;
    private IonType next;

    protected ReaderIterator(IonSystemLite system, IonReader reader)
    {
        this.reader = reader;
        this.system = system;
    }

    public boolean hasNext()
    {
        if (next == null) {
            next = reader.next();
        }

        boolean hasNext = next != null;

        if (!hasNext) {
            try {
                close();
            }
            catch (IOException e) {
                throw new IonException(e);
            }
        }

        return hasNext;
    }

    public IonValue next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        SymbolTable symtab = reader.getSymbolTable();

        // make an ion value from our reader
        // We already called reader.next() inside hasNext() above
        IonValueLite value = system.newValue(reader);

        // we've used up the value now, force a reader.next() the next time through
        next = null;

        value.setSymbolTable(symtab);

        return value;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException
    {
        reader.close();
    }
}
