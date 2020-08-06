package tools.errorReport;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;

import java.io.IOException;

public class ErrorDescription {
    private final ErrorType errorType;
    private final String message;
    private final String location;
    private final int eventIndex;

    public ErrorDescription(ErrorType errorType, String message, String location, int eventIndex) {
        this.errorType = errorType;
        this.message = message;
        this.location = location;
        this.eventIndex = eventIndex;
    }

    public ErrorDescription(ErrorType errorType, String message, String location) {
        this.errorType = errorType;
        this.message = message;
        this.location = location;
        this.eventIndex = -99;
    }

    public void writeOutput(IonWriter ionWriterForErrorReport) throws IOException {
        ionWriterForErrorReport.stepIn(IonType.STRUCT);
        if (this.errorType != null) {
            ionWriterForErrorReport.setFieldName("error_type");
            ionWriterForErrorReport.writeSymbol(this.errorType.toString());
        }
        if (this.message != null) {
            ionWriterForErrorReport.setFieldName("message");
            ionWriterForErrorReport.writeString(this.message);
        }
        if (this.location != null) {
            ionWriterForErrorReport.setFieldName("location");
            ionWriterForErrorReport.writeString(this.location);
        }

        if(this.eventIndex != -99) {
            ionWriterForErrorReport.setFieldName("event_index");
            ionWriterForErrorReport.writeInt(this.eventIndex);
        }

        ionWriterForErrorReport.stepOut();
    }
}
