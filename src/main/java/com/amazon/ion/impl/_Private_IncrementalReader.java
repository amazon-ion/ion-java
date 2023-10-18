package com.amazon.ion.impl;

/**
 * Interface to be implemented by all incremental IonReaders. See
 * {@link com.amazon.ion.system.IonReaderBuilder#withIncrementalReadingEnabled(boolean)}.
 */
public interface _Private_IncrementalReader {

    /**
     * Requires that the reader not currently be buffering an incomplete value.
     * @throws com.amazon.ion.IonException if the reader is buffering an incomplete value.
     */
    void requireCompleteValue();
}
