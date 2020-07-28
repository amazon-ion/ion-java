package com.amazon.tools.cli;

import com.amazon.tools.events.Event;

import java.util.List;

public class CompareContext {
    String path;
    String compareToPath;
    int fileEventIndex;
    int compareToFileEventIndex;
    String message;
    List<Event> eventStreamFirst;
    List<Event> eventStreamSecond;
    ComparisonType type;

    public CompareContext(List<Event> eventStreamFirst,
                          List<Event> eventStreamSecond) {
        this.path = null;
        this.compareToPath = null;
        this.eventStreamFirst = eventStreamFirst;
        this.eventStreamSecond = eventStreamSecond;
        this.message = null;
        this.type = null;
    }

    public void reset(String file, String compareToFile){
        this.setFile(file);
        this.setCompareToFile(compareToFile);
        this.setFileEventIndex(-1);
        this.setCompareToFileEventIndex(-1);
        this.message = null;
        this.eventStreamFirst = null;
        this.eventStreamSecond = null;
    }

    public String getFile() {
        return path;
    }

    public void setFile(String file) {
        this.path = file;
    }

    public String getCompareToFile() {
        return compareToPath;
    }

    public void setCompareToFile(String compareToFile) {
        this.compareToPath = compareToFile;
    }

    public int getFileEventIndex() {
        return fileEventIndex;
    }

    public void setFileEventIndex(int fileEventIndex) {
        this.fileEventIndex = fileEventIndex;
    }

    public int getCompareToFileEventIndex() {
        return compareToFileEventIndex;
    }

    public void setCompareToFileEventIndex(int compareToFileEventIndex) {
        this.compareToFileEventIndex = compareToFileEventIndex;
    }

    public List<Event> getEventStreamFirst() {
        return eventStreamFirst;
    }

    public void setEventStreamFirst(List<Event> eventStreamFirst) {
        this.eventStreamFirst = eventStreamFirst;
    }

    public List<Event> getEventStreamSecond() {
        return eventStreamSecond;
    }

    public void setEventStreamSecond(List<Event> eventStreamSecond) {
        this.eventStreamSecond = eventStreamSecond;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public ComparisonType getType() {
        return type;
    }

    public void setType(ComparisonType type) {
        this.type = type;
    }
}
