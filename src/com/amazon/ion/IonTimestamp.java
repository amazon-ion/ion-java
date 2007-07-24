/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.util.Date;

// TODO document local offset viz RFC-3339

/**
 * An Ion <code>timestamp</code> value.
 */
public interface IonTimestamp
    extends IonValue
{
    /**
     * Gets the value of this Ion <code>timestamp</code> as a Java
     * {@link Date}, representing the time in UTC.  As a result, this method
     * will return the same result for all Ion representations of the same
     * instant, regardless of the local offset.
     * <p>
     * Because <code>Date</code> instances are mutable, this method returns a
     * new instance from each call.
     *
     * @return a new <code>Date</code> value, in UTC,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     */
    public Date dateValue();


    /**
     * Gets the value of this Ion <code>timestamp</code> as the number of 
     * milliseconds since 1970-01-01T00:00:00Z.  This method
     * will return the same result for all Ion representations of the same
     * instant, regardless of the local offset.
     *
     * @return the number of milliseconds since 1970-01-01T00:00:00Z
     * represented by this timestamp.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public long getMillis()
        throws NullValueException;


    /**
     * Sets this timestamp to represent the point in time that is 
     * <code>millis</code> milliseconds after 1970-01-01T00:00:00Z. 
     * <p>
     * This method does not change the local offset part.
     * If this is <code>null.timestamp</code>, then the local offset will be
     * unknown.
     *
     * @param millis the number of milliseconds since 1970-01-01T00:00:00Z to
     * be represented by this timestamp.
     * @throws IllegalArgumentException if the resulting timestamp would
     * precede 0001-01-01T00:00:00Z.
     */
    public void setMillis(long millis);


    /**
     * Sets this timestamp to represent the point in time that is 
     * <code>millis</code> milliseconds after 1970-01-01T00:00:00Z,
     * and sets the local offset to UTC.
     *
     * @param millis the number of milliseconds since 1970-01-01T00:00:00Z to
     * be represented by this timestamp.
     */
    public void setMillisUtc(long millis);

    
    /**
     * Gets the local offset (in minutes) of this timestamp, or <code>null</code> if
     * it's unknown (<em>i.e.</em>, <code>-00:00</code>).
     * <p>
     * For example, the result for <code>1969-02-23T07:00+07:00</code> is 420,
     * the result for <code>1969-02-22T22:45:00.00-01:15</code> is -75, and
     * the result for <code>1969-02-23</code> (by Ion's definition, equivalent
     * to <code>1969-02-23T00:00-00:00</code>) is <code>null</code>.
     *
     * @return the positive or negative local-time offset, represented as
     * minutes from UTC.  If the offset is unknown, returns <code>null</code>.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public Integer getLocalOffset()
        throws NullValueException;


    /**
     * Sets the time portion of this timestamp.
     * If <code>value</code> is <code>null</code>, then this will become
     * <code>null.timestamp</code>.
     * If this is <code>null.timestamp</code>, then the local offset will be
     * unknown.
     *
     * @param value will be copied into this element. If null then this
     * becomes <code>null.timestamp</code>.
     */
    public void setTime(Date value);

    
    /**
     * Sets the time portion of this timestamp to the current time, leaving the
     * local offset portion unchanged.
     * <p>
     * If <code>this.isNullValue()</code>, then the local offset will be
     * unknown.
     */
    public void setCurrentTime();

    
    /**
     * Sets the time portion of this timestamp to the current time, and the
     * local offset portion to UTC.
     */
    public void setCurrentTimeUtc();
    

    /**
     * Sets the local-offset portion of this timestamp.  The time portion is
     * not changed.  Note that this method cannot set the unknown offset
     * (<code>-00:00</code>); to do that use {@link #setLocalOffset(Integer)}.
     *
     * @param minutes is the new local offset, in minutes.  Zero indicates UTC.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public void setLocalOffset(int minutes)
        throws NullValueException;


    /**
     * Sets the local-offset portion of this timestamp.  The time portion is
     * not changed.
     *
     * @param minutes is the new local offset, in minutes.  Zero indicates UTC.
     * <code>null</code> indicates the unknown offset (<code>-00:00</code>).
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public void setLocalOffset(Integer minutes)
        throws NullValueException;
}
