package software.amazon.ion;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterator that can close its source of data.
 *
 * @param <T> the type of elements returned by this iterator.
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable
{
    // empty
}
