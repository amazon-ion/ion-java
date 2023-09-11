package com.amazon.tools.cli;

import com.amazon.ion.IonWriter;
import com.amazon.tools.errorReport.ErrorType;
import com.amazon.tools.events.Event;
import com.amazon.tools.events.EventType;

import java.io.File;
import java.util.List;

public class ProcessContext {
    private String fileName;
    private int eventIndex;
    private EventType lastEventType;
    private ErrorType state;
    private IonWriter ionWriter;
    private File file;
    private StringBuilder embeddedOut;
    private List<Event> eventStream;

    public ProcessContext(String file, int index, EventType lastEventType, ErrorType state, IonWriter ionWriter) {
        this.fileName = file;
        this.eventIndex = index;
        this.lastEventType = lastEventType;
        this.state = state;
        this.ionWriter = ionWriter;
        this.file = null;
        this.embeddedOut = null;
        this.eventStream = null;
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

    public File getFile() {
        return file;
    }

    public void setFile(File file) {
        this.file = file;
    }

    public StringBuilder getEmbeddedOut() {
        return embeddedOut;
    }

    public void setEmbeddedOut(StringBuilder embeddedOut) {
        this.embeddedOut = embeddedOut;
    }

    public List<Event> getEventStream() {
        return eventStream;
    }

    public void setEventStream(List<Event> eventStream) {
        this.eventStream = eventStream;
    }
}
