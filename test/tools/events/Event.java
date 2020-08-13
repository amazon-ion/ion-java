package tools.events;

import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonWriter;

import com.amazon.ion.system.IonReaderBuilder;
import tools.cli.ProcessContext;
import tools.errorReport.ErrorDescription;
import tools.errorReport.ErrorType;

import java.io.IOException;

public class Event {
    private static final int IO_ERROR_EXIT_CODE = 2;

    private EventType eventType;
    private IonType ionType;
    private SymbolToken fieldName;
    private SymbolToken[] annotations;
    private String valueText;
    private byte[] valueBinary;
    private ImportDescriptor[] imports;
    private int depth;

    public Event() {
        this.eventType = null;
        this.ionType = null;
        this.fieldName = null;
        this.annotations = null;
        this.valueText = null;
        this.valueBinary = null;
        this.imports = null;
        this.depth = -1;
    }

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

    public void validate() throws IonException, IOException {
        if (this.eventType == null) throw new IonException("event_type can't be null");
        else if (this.ionType == null && this.eventType != EventType.STREAM_END)
            throw new IonException("ion_type can't be null");

        EventType eventType = this.eventType;
        switch (eventType) {
            case CONTAINER_START:
                if (!IonType.isContainer(this.ionType)) {
                    throw new IonException("Invalid event_type: not a container");
                }
                break;
            case SCALAR:
                String textValue = this.getValueText();
                byte[] binaryValue = this.getValueBinary();
                if (textValue == null && binaryValue == null) {
                } else if (textValue != null && binaryValue != null) {
                    try (
                            IonReader ionReaderX = IonReaderBuilder.standard().build(textValue);
                            IonReader ionReaderY = IonReaderBuilder.standard().build(binaryValue);
                    ) {
                        ionReaderX.next();
                        ionReaderY.next();

                        if (!validateTwoIonReaderValue(ionReaderX, ionReaderY)) {
                            throw new IonException("invalid Event: Text value and Binary value are different");
                        }
                    }
                } else {
                    throw new IonException("invalid Event: Text value and Binary value are different");
                }
                break;
            case SYMBOL_TABLE:
            case CONTAINER_END:
            case STREAM_END:
                break;
            default:
                throw new IonException("Invalid event_type");
        }
    }

    public boolean validateTwoIonReaderValue(IonReader x, IonReader y) {
        if (x.getType() != y.getType()) return false;
        IonType type = x.getType();

        switch (type) {
            case NULL:
                return true;
            case BOOL:
                return x.booleanValue() == y.booleanValue();
            case INT:
                return x.intValue() == y.intValue();
            case FLOAT:
            case DECIMAL:
                return x.doubleValue() == y.doubleValue();
            case TIMESTAMP:
                return x.timestampValue().equals(y.timestampValue());
            case SYMBOL:
                SymbolToken xSymbol = x.symbolValue();
                SymbolToken ySymbol = y.symbolValue();
                return xSymbol.getText().equals(ySymbol.getText());
            case STRING:
                return x.stringValue().equals(y.stringValue());
            case CLOB:
            case BLOB:
                byte[] xByte = x.newBytes();
                byte[] yByte = y.newBytes();
                int xLength = xByte.length;
                int yLength = yByte.length;

                if (xLength == yLength) {
                    for (int i = 0 ; i < xLength; i++) {
                        if (xByte[i] != yByte[i]) return false;
                    }
                } else {
                    return false;
                }
                return true;
            default:
                throw new IonException("invalid ion_type " + ionType.toString());
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

            if (this.valueText != null && this.valueBinary != null) {
                ionWriterForOutput.setFieldName("value_text");
                ionWriterForOutput.writeString(this.valueText);

                ionWriterForOutput.setFieldName("value_binary");
                ionWriterForOutput.stepIn(IonType.LIST);
                for (byte b : this.valueBinary) {
                    ionWriterForOutput.writeInt(b & 0xff);
                }
                ionWriterForOutput.stepOut();
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

    public String getValueText() {
        return valueText;
    }

    public byte[] getValueBinary() {
        return valueBinary;
    }

    public ImportDescriptor[] getImports() {
        return imports;
    }

    public int getDepth() {
        return depth;
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

    public void setValueText(String valueText) {
        this.valueText = valueText;
    }

    public void setValueBinary(byte[] valueBinary) {
        this.valueBinary = valueBinary;
    }

    public void setImports(ImportDescriptor[] imports) {
        this.imports = imports;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }
}
