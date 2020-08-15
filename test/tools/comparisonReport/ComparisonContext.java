package tools.comparisonReport;

import tools.events.Event;

public class ComparisonContext {
    private final String location;
    private final Event event;
    private final int eventIndex;

    public ComparisonContext(String location, Event event, int eventIndex) {
        this.location = location;
        this.event = event;
        this.eventIndex = eventIndex;
    }

    public String getLocation() {
        return location;
    }

    public Event getEvent() {
        return event;
    }

    public int getEventIndex() {
        return eventIndex;
    }
}
