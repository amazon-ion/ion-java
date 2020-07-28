package tools.cli;

import com.amazon.ion.IonWriter;

import java.io.OutputStream;

/**
 * Implementors of this class (namely {@link OutputFormat}) can produce an appropriately configured IonWriter
 * given only the OutputStream destination.
 */
public interface IonWriterCreator {
    IonWriter createIonWriter(OutputStream outputStream);
}