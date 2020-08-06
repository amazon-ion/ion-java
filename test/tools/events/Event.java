package tools.events;

import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolToken;
import tools.errorReport.ErrorDescription;
import tools.errorReport.ErrorType;

import java.io.IOException;

public class Event {
    private static final int IO_ERROR_EXIT_CODE = 2;

    private final EventType eventType;
    private final IonType ionType;
    private final SymbolToken fieldName;
    private final SymbolToken[] annotations;
    private final String valueText;
    private final byte[] valueBinary;
    private final ImportDescriptor[] imports;
    private final int depth;

    public Event(EventType eventType, IonType ionType, SymbolToken fieldName, SymbolToken[] annotations,
                 String valueText, byte[] valueBinary, ImportDescriptor[] imports, int depth) {
        this.eventType = eventType;
        this.ionType = ionType;
        this.fieldName = fieldName;
        this.annotations = annotations;
        this.valueText = valueText;
        this.valueBinary = valueBinary;
        this.imports = imports;
        this.depth = depth;
    }

    /**
     * write Event structure to Ion Stream and return the updated eventIndex.
     */
    public int writeOutput(IonWriter ionWriterForOutput,
                           IonWriter ionWriterForErrorReport,
                           String fileName,
                           int eventIndex) throws IOException {
        try {
            ionWriterForOutput.stepIn(IonType.STRUCT);
            if (this.eventType != null) {
                ionWriterForOutput.setFieldName("event_type");
                ionWriterForOutput.writeSymbol(this.eventType.toString());
            }
            if (this.ionType != null) {
                ionWriterForOutput.setFieldName("ion_type");
                ionWriterForOutput.writeSymbol(this.ionType.toString());
            }
            if (this.fieldName != null) {
                ionWriterForOutput.setFieldName("field_name");
                ionWriterForOutput.stepIn(IonType.STRUCT);
                ionWriterForOutput.setFieldName("text");
                ionWriterForOutput.writeString(this.fieldName.getText());
                ionWriterForOutput.stepOut();
            }
            if (this.annotations != null && this.annotations.length > 0) {
                ionWriterForOutput.setFieldName("annotations");
                ionWriterForOutput.stepIn(IonType.LIST);
                for (int i = 0; i < this.annotations.length; i++) {
                    ionWriterForOutput.stepIn(IonType.STRUCT);
                    ionWriterForOutput.setFieldName("text");
                    String text = this.annotations[i].getText();
                    if (text == null) {
                        ionWriterForOutput.writeNull();
                        ionWriterForOutput.setFieldName("import_location");
                        ionWriterForOutput.writeNull();
                    } else {
                        ionWriterForOutput.writeString(text);
                    }
                    ionWriterForOutput.stepOut();
                }
                ionWriterForOutput.stepOut();
            }

            if(this.valueText != null && this.valueBinary != null) {
                ionWriterForOutput.setFieldName("value_text");
                ionWriterForOutput.writeString(this.valueText);

                ionWriterForOutput.setFieldName("value_binary");
                ionWriterForOutput.stepIn(IonType.LIST);
                for (int i = 0; i < this.valueBinary.length; i++) {
                    ionWriterForOutput.writeInt(this.valueBinary[i] & 0xff);
                }
                ionWriterForOutput.stepOut();
            }

            if (this.imports != null && this.imports.length > 0) {
                ionWriterForOutput.setFieldName("imports");
                ionWriterForOutput.stepIn(IonType.LIST);
                for (int i = 0; i < this.imports.length; i++) {
                    ionWriterForOutput.stepIn(IonType.STRUCT);
                    ionWriterForOutput.setFieldName("import_name");
                    ionWriterForOutput.writeString(this.imports[i].getImportName());
                    ionWriterForOutput.setFieldName("version");
                    ionWriterForOutput.writeInt(this.imports[i].getVersion());
                    ionWriterForOutput.setFieldName("max_id");
                    ionWriterForOutput.writeInt(this.imports[i].getMaxId());
                    ionWriterForOutput.stepOut();
                }
                ionWriterForOutput.stepOut();
            }
            ionWriterForOutput.setFieldName("depth");
            ionWriterForOutput.writeInt(this.depth);
            ionWriterForOutput.stepOut();
        } catch (IonException e) {
            new ErrorDescription(ErrorType.WRITE, e.getMessage(), fileName, eventIndex)
                    .writeOutput(ionWriterForErrorReport);
            System.exit(IO_ERROR_EXIT_CODE);
        }

        //event index + 1 if we write OutputStream successfully.
        return eventIndex + 1;
    }
}