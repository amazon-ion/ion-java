package tools.cli;

import tools.errorReport.ErrorType;
import tools.events.EventType;

public class ProcessContext {
    private String fileName;
    private int eventIndex;
    private EventType lastEventType;
    private ErrorType state;

    public ProcessContext(String file, int index, EventType lastEventType, ErrorType state) {
        this.fileName = file;
        this.eventIndex = index;
        this.lastEventType = lastEventType;
        this.state = state;
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

}
