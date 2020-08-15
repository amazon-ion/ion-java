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
            ionWriter.setFieldName("lhs");
            ionWriter.stepIn(IonType.STRUCT);
            ionWriter.setFieldName("location");
            ionWriter.writeString(this.lhs.getLocation());
            ionWriter.setFieldName("event");
            this.lhs.getEvent().writeOutput(ionWriter, -1);
            ionWriter.setFieldName("eventIndex");
            ionWriter.writeInt(this.lhs.getEventIndex());
            ionWriter.stepOut();
        }
        if (this.rhs != null) {
            ionWriter.setFieldName("rhs");
            ionWriter.stepIn(IonType.STRUCT);
            ionWriter.setFieldName("location");
            ionWriter.writeString(this.rhs.getLocation());
            ionWriter.setFieldName("event");
            this.rhs.getEvent().writeOutput(ionWriter, -1);
            ionWriter.setFieldName("eventIndex");
            ionWriter.writeInt(this.rhs.getEventIndex());
            ionWriter.stepOut();
        }
        ionWriter.stepOut();
    }
}
