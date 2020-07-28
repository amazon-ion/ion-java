package tools.events;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import com.amazon.ion.SymbolToken;

import java.io.IOException;

public class Event {
    public EventType eventType;
    public IonType ionType;
    public SymbolToken field_name;
    public SymbolToken[] annotations;
    public String value_text;
    public byte[] value_binary;
    public ImportDescriptor[] imports;
    public int depth;

    public Event(EventType eventType, IonType ionType, SymbolToken field_name, SymbolToken[] annotations,
          String value_text, byte[] value_binary, ImportDescriptor[] imports, int depth) {
        this.eventType = eventType;
        this.ionType = ionType;
        this.field_name = field_name;
        this.annotations = annotations;
        this.value_text = value_text;
        this.value_binary = value_binary;
        this.imports = imports;
        this.depth = depth;
    }

    public void writeOutput(IonWriter ionWriterForOutput, IonWriter ionWriterForErrorReport) throws IOException {
        ionWriterForOutput.stepIn(IonType.STRUCT);
        if(this.eventType != null){
            ionWriterForOutput.setFieldName("event_type");
            ionWriterForOutput.writeString(this.eventType.toString());
        }
        if(this.ionType != null){
            ionWriterForOutput.setFieldName("ion_type");
            ionWriterForOutput.writeString(this.ionType.toString());
        }
        if(this.field_name != null) {
            ionWriterForOutput.setFieldName("field_name");
            ionWriterForOutput.stepIn(IonType.STRUCT);
            ionWriterForOutput.setFieldName("text");
            ionWriterForOutput.writeString(this.field_name.getText());
            ionWriterForOutput.stepOut();
        }
        if(this.annotations != null && this.annotations.length > 0) {
            ionWriterForOutput.setFieldName("annotations");
            ionWriterForOutput.stepIn(IonType.LIST);
            for(int i=0; i<this.annotations.length; i++) {
                ionWriterForOutput.stepIn(IonType.STRUCT);
                ionWriterForOutput.setFieldName("text");
                String text = this.annotations[i].getText();
                ionWriterForOutput.writeString(text);
                if(text == null) {
                    ionWriterForOutput.setFieldName("import_location");
                    ionWriterForOutput.writeString(null);
                }
                ionWriterForOutput.stepOut();
            }
            ionWriterForOutput.stepOut();
        }
        if(this.value_text != null) {
            ionWriterForOutput.setFieldName("value_text");
            ionWriterForOutput.writeString(this.value_text);
        }
        if(this.value_binary != null && this.value_binary.length>0) {
            ionWriterForOutput.setFieldName("value_binary");
            ionWriterForOutput.stepIn(IonType.LIST);
            for(int i=0; i<this.value_binary.length; i++){
                ionWriterForOutput.writeInt(this.value_binary[i] & 0xff);
            }
            ionWriterForOutput.stepOut();
        }
        if(this.imports != null && this.imports.length > 0) {
            ionWriterForOutput.setFieldName("imports");
            ionWriterForOutput.stepIn(IonType.LIST);
            for(int i=0; i<this.imports.length; i++){
                ionWriterForOutput.stepIn(IonType.STRUCT);
                ionWriterForOutput.setFieldName("import_name");
                ionWriterForOutput.writeString(this.imports[i].getImport_name());
                ionWriterForOutput.setFieldName("version");
                ionWriterForOutput.writeInt(this.imports[i].getVersion());
                ionWriterForOutput.setFieldName("max_id");
                ionWriterForOutput.writeInt(this.imports[i].getMax_id());
                ionWriterForOutput.stepOut();
            }
            ionWriterForOutput.stepOut();
        }
        ionWriterForOutput.setFieldName("depth");
        ionWriterForOutput.writeInt(this.depth);
        ionWriterForOutput.stepOut();
    }
}