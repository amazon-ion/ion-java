package tools.events;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolToken;

import java.io.IOException;

public class Event {
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

    public void writeOutput(IonWriter ionWriterForOutput, IonWriter ionWriterForErrorReport) throws IOException {
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
        if (this.valueText != null) {
            ionWriterForOutput.setFieldName("value_text");
            ionWriterForOutput.writeString(this.valueText);
        }
        if (this.valueBinary != null && this.valueBinary.length > 0) {
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
    }
}