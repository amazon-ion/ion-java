package tools.cli;

import com.amazon.ion.IonWriter;
import tools.errorReport.ErrorType;
import tools.events.EventType;

public class ProcessContext {
    private String fileName;
    private int eventIndex;
    private EventType lastEventType;
    private ErrorType state;
    private IonWriter ionWriter;

    public ProcessContext(String file, int index, EventType lastEventType, ErrorType state, IonWriter ionWriter) {
        this.fileName = file;
        this.eventIndex = index;
        this.lastEventType = lastEventType;
        this.state = state;
        this.ionWriter = ionWriter;
    }

    public String getFileName() {
        return this.fileName;
    }

    public int getEventIndex() {
        return this.eventIndex;
    }

    public EventType getLastEventType() {
        return this.lastEventType;
    }

    public ErrorType getState() {
        return state;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setEventIndex(int eventIndex) {
        this.eventIndex = eventIndex;
    }

    public void setLastEventType(EventType lastEventType) {
        this.lastEventType = lastEventType;
    }

    public void setState(ErrorType state) {
        this.state = state;
    }

    public IonWriter getIonWriter() {
        return ionWriter;
    }

    public void setIonWriter(IonWriter ionWriter) {
        this.ionWriter = ionWriter;
    }
}
