package tools.cli;

import tools.events.EventType;

public class ProcessContext {
    private String fileName;
    private int eventIndex;
    private EventType lastEventType;

    public ProcessContext(String file, int index, EventType lastEventType) {
        this.fileName = file;
        this.eventIndex = index;
        this.lastEventType = lastEventType;
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

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setEventIndex(int eventIndex) {
        this.eventIndex = eventIndex;
    }

    public void setLastEventType(EventType lastEventType) {
        this.lastEventType = lastEventType;
    }

}
