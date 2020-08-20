package tools.comparisonReport;

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
            ionWriter.writeString((result.toString()));
        }
        if (this.message != null) {
            ionWriter.setFieldName("message");
            ionWriter.writeString((message));
        }
        if (this.lhs != null) {
            writeComparisonContext(ionWriter, true);
        }
        if (this.rhs != null) {
            writeComparisonContext(ionWriter, false);
        }
        ionWriter.stepOut();
    }

    private void writeComparisonContext(IonWriter ionWriter, boolean isLeft) throws IOException {
        ionWriter.setFieldName(isLeft ? "lhs" : "rhs");
        ionWriter.stepIn(IonType.STRUCT);
        ionWriter.setFieldName("location");
        ionWriter.writeString(isLeft ? this.lhs.getLocation() : this.rhs.getLocation());
        ionWriter.setFieldName("event");
        if (isLeft) {
            this.lhs.getEvent().writeOutput(ionWriter, -1);
        } else {
            this.rhs.getEvent().writeOutput(ionWriter, -1);
        }
        ionWriter.setFieldName("eventIndex");
        ionWriter.writeInt(isLeft ? this.lhs.getEventIndex() : this.rhs.getEventIndex());
        ionWriter.stepOut();
    }
}
