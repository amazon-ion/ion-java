package tools.events;

import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.IonException;
import com.amazon.ion.IonWriter;

import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import tools.cli.ProcessContext;
import tools.errorReport.ErrorDescription;
import tools.errorReport.ErrorType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Event {
    private static final int IO_ERROR_EXIT_CODE = 2;

    private EventType eventType;
    private IonType ionType;
    private SymbolToken fieldName;
    private SymbolToken[] annotations;
    private IonValue value;
    private ImportDescriptor[] imports;
    private int depth;

    public Event() {
        this.eventType = null;
        this.ionType = null;
        this.fieldName = null;
        this.annotations = null;
        this.value = null;
        this.imports = null;
        this.depth = -1;
    }

    public Event(EventType eventType, IonType ionType, SymbolToken fieldName, SymbolToken[] annotations,
                 IonValue value, ImportDescriptor[] imports, int depth) {
        this.eventType = eventType;
        this.ionType = ionType;
        this.fieldName = fieldName;
        this.annotations = annotations;
        this.value = value;
        this.imports = imports;
        this.depth = depth;
    }

    public void validate() throws IonException, IOException {
        if (this.eventType == null) throw new IonException("event_type can't be null");
        else if (this.ionType == null
                && (this.eventType != EventType.STREAM_END
                    && this.eventType != EventType.SYMBOL_TABLE))
            throw new IonException("ion_type can't be null");

        EventType eventType = this.eventType;
        switch (eventType) {
            case CONTAINER_START:
                if (!IonType.isContainer(this.ionType)) {
                    throw new IonException("Invalid event_type: not a container");
                }
                break;
            case SCALAR:
            case SYMBOL_TABLE:
            case CONTAINER_END:
            case STREAM_END:
                break;
            default:
                throw new IonException("Invalid event_type");
        }
    }

    public void writeOutput(IonWriter ionWriterForOutput,
                            IonWriter ionWriterForErrorReport,
                            ProcessContext processContext) throws IOException {
        int updatedEventIndex = writeOutput(ionWriterForOutput, ionWriterForErrorReport,
                processContext.getFileName(),processContext.getEventIndex());

        processContext.setEventIndex(updatedEventIndex);
    }

    /**
     * write Event structure to Event Stream and return the updated eventIndex.
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
                if (this.fieldName.getText() == null) {
                    ionWriterForOutput.writeNull();
                } else {
                    ionWriterForOutput.writeString(this.fieldName.getText());
                }
                ionWriterForOutput.setFieldName("import_location");
                ionWriterForOutput.writeNull();
                ionWriterForOutput.stepOut();
            }
            if (this.annotations != null && this.annotations.length > 0) {
                ionWriterForOutput.setFieldName("annotations");
                ionWriterForOutput.stepIn(IonType.LIST);
                for (SymbolToken annotation : this.annotations) {
                    ionWriterForOutput.stepIn(IonType.STRUCT);
                    ionWriterForOutput.setFieldName("text");
                    String text = annotation.getText();
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

            if (this.value != null) {
                String valueText;
                byte[] valueBinary;
                try (
                        ByteArrayOutputStream textOut = new ByteArrayOutputStream();
                        IonWriter textWriter = IonTextWriterBuilder.standard().build(textOut);
                        ByteArrayOutputStream binaryOut = new ByteArrayOutputStream();
                        IonWriter binaryWriter = IonBinaryWriterBuilder.standard().build(binaryOut);
                ) {
                    //write Text
                    this.value.writeTo(textWriter);
                    textWriter.finish();
                    valueText = textOut.toString("utf-8");

                    ionWriterForOutput.setFieldName("value_text");
                    ionWriterForOutput.writeString(valueText);


                    //write binary
                    this.value.writeTo(binaryWriter);
                    binaryWriter.finish();
                    valueBinary = binaryOut.toByteArray();

                    ionWriterForOutput.setFieldName("value_binary");
                    ionWriterForOutput.stepIn(IonType.LIST);
                    for (byte b : valueBinary) {
                        ionWriterForOutput.writeInt(b & 0xff);
                    }
                    ionWriterForOutput.stepOut();
                }
            }


            if (this.imports != null && this.imports.length > 0) {
                ionWriterForOutput.setFieldName("imports");
                ionWriterForOutput.stepIn(IonType.LIST);
                for (ImportDescriptor anImport : this.imports) {
                    ionWriterForOutput.stepIn(IonType.STRUCT);
                    ionWriterForOutput.setFieldName("import_name");
                    ionWriterForOutput.writeString(anImport.getImportName());
                    ionWriterForOutput.setFieldName("version");
                    ionWriterForOutput.writeInt(anImport.getVersion());
                    ionWriterForOutput.setFieldName("max_id");
                    ionWriterForOutput.writeInt(anImport.getMaxId());
                    ionWriterForOutput.stepOut();
                }
                ionWriterForOutput.stepOut();
            }
            if (this.depth != -1) {
                ionWriterForOutput.setFieldName("depth");
                ionWriterForOutput.writeInt(this.depth);
            }
            ionWriterForOutput.stepOut();
        } catch (IonException e) {
            new ErrorDescription(ErrorType.WRITE, e.getMessage(), fileName, eventIndex)
                    .writeOutput(ionWriterForErrorReport);
            System.exit(IO_ERROR_EXIT_CODE);
        }

        //event index + 1 if we write OutputStream successfully.
        return eventIndex + 1;
    }

    public EventType getEventType() {
        return eventType;
    }

    public IonType getIonType() {
        return ionType;
    }

    public SymbolToken getFieldName() {
        return fieldName;
    }

    public SymbolToken[] getAnnotations() {
        return annotations;
    }

    public ImportDescriptor[] getImports() {
        return imports;
    }

    public int getDepth() {
        return depth;
    }

    public IonValue getValue() {
        return value;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }

    public void setIonType(IonType ionType) {
        this.ionType = ionType;
    }

    public void setFieldName(SymbolToken fieldName) {
        this.fieldName = fieldName;
    }

    public void setAnnotations(SymbolToken[] annotations) {
        this.annotations = annotations;
    }

    public void setValue(IonValue value) {
        this.value = value;
    }

    public void setImports(ImportDescriptor[] imports) {
        this.imports = imports;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }
}
