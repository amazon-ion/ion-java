package tools.cli;

import tools.events.Event;

import java.util.ArrayList;

public class CompareContext {
    String path;
    String compareToPath;
    int fileEventIndex;
    int compareToFileEventIndex;
    ArrayList<Event> eventStreamFirst;
    ArrayList<Event> eventStreamSecond;
    String message;


    public CompareContext(ArrayList<Event> eventStreamFirst,
                          ArrayList<Event> eventStreamSecond) {
        this.path = null;
        this.compareToPath = null;
        this.eventStreamFirst = eventStreamFirst;
        this.eventStreamSecond = eventStreamSecond;
        this.message = null;
    }

    public void reset(String file, String compareToFile){
        this.setFile(file);
        this.setCompareToFile(compareToFile);
        this.setFileEventIndex(1);
        this.setCompareToFileEventIndex(1);
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

    public ArrayList<Event> getEventStreamFirst() {
        return eventStreamFirst;
    }

    public void setEventStreamFirst(ArrayList<Event> eventStreamFirst) {
        this.eventStreamFirst = eventStreamFirst;
    }

    public ArrayList<Event> getEventStreamSecond() {
        return eventStreamSecond;
    }

    public void setEventStreamSecond(ArrayList<Event> eventStreamSecond) {
        this.eventStreamSecond = eventStreamSecond;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
