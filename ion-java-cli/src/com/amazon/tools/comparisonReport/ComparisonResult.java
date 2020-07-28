package com.amazon.tools.comparisonReport;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;

import java.io.IOException;

public class ComparisonResult {
    private final ComparisonResultType result;
    private final ComparisonContext lhs;
    private final ComparisonContext rhs;
    private final String message;

    public ComparisonResult(ComparisonResultType result, ComparisonContext lhs,
                            ComparisonContext rhs, String message) {
        this.result = result;
        this.lhs = lhs;
        this.rhs = rhs;
        this.message = message;
    }

    public void writeOutput(IonWriter ionWriter) throws IOException {
        ionWriter.stepIn(IonType.STRUCT);
        if (this.result != null) {
            ionWriter.setFieldName("result");
            ionWriter.writeString(result.toString());
        }
        if (this.message != null) {
            ionWriter.setFieldName("message");
            ionWriter.writeString(message);
        }
        if (this.lhs != null) {
            ionWriter.setFieldName("lhs");
            writeComparisonContext(ionWriter, this.lhs);
        }
        if (this.rhs != null) {
            ionWriter.setFieldName("rhs");
            writeComparisonContext(ionWriter, this.rhs);
        }
        ionWriter.stepOut();
    }

    private void writeComparisonContext(IonWriter ionWriter, ComparisonContext comparisonContext) throws IOException {
        ionWriter.stepIn(IonType.STRUCT);
        ionWriter.setFieldName("location");
        ionWriter.writeString(comparisonContext.getLocation());
        if (comparisonContext.getEvent() != null) {
            ionWriter.setFieldName("event");
            comparisonContext.getEvent().writeOutput(ionWriter, -1);
        }
        ionWriter.setFieldName("event_index");
        ionWriter.writeInt(comparisonContext.getEventIndex());
        ionWriter.stepOut();
    }
}
