package tools.cli;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;

public class IonJavaTestUtil {
    private static final String EVENT_STREAM = "$ion_event_stream";

    public static boolean isEventStream(IonReader ionReader) {
        return ionReader.next() != null
                && ionReader.getType() == IonType.SYMBOL
                && EVENT_STREAM.equals(ionReader.symbolValue().getText());
    }

}
