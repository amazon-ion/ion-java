/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion;

import java.math.BigDecimal;
import java.util.Date;

// TODO document local offset viz RFC-3339

/**
 * An Ion <code>timestamp</code> value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonTimestamp
    extends IonValue
{
    // TODO amznlabs/ion-java#33 Deprecate setters and getters

    /**
     * Gets the value of this <code>timestamp</code> in a form suitable for
     * use independent of Ion data.
     *
     * @return the value of this timestamp,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     */
    public Timestamp timestampValue();


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
     * milliseconds since 1970-01-01T00:00:00.000Z, truncating any fractional
     * milliseconds.
     * This method will return the same result for all Ion representations of
     * the same instant, regardless of the local offset.
     *
     * @return the number of milliseconds since 1970-01-01T00:00:00.000Z
     * represented by this timestamp.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     */
    public long getMillis()
        throws NullValueException;

    /**
     * Gets the value of this Ion <code>timestamp</code> as the number of
     * milliseconds since 1970-01-01T00:00:00Z, including fractional
     * milliseconds.
     * This method will return the same result for all Ion representations of
     * the same instant, regardless of the local offset.
     *
     * @return the number of milliseconds since 1970-01-01T00:00:00Z
     * represented by this timestamp,
     * or <code>null</code> if <code>this.isNullValue()</code>.
     */
    public BigDecimal getDecimalMillis();


    /**
     * Sets the value of this {@code timestamp}.
     *
     * @param timestamp may be {@code null} to cause this to be
     * {@code null.timestamp}.
     */
    public void setValue(Timestamp timestamp);


    /**
     * Sets this timestamp to represent the point in time that is
     * {@code millis} milliseconds after 1970-01-01T00:00:00Z, with
     * the specified local offset of {@code localOffset}.
     *
     * @param millis
     *                  the number of milliseconds since 1970-01-01T00:00:00Z to
     *                  be represented by this timestamp.
     * @param localOffset
     *                  the local offset of this timestamp, in minutes. Zero
     *                  indicates UTC. {@code null} indicates the unknown
     *                  offset ({@code -00:00}).
     *
     * @throws IllegalArgumentException
     *                  if the resulting timestamp would precede
     *                  0001-01-01T00:00:00Z.
     * @throws NullPointerException
     *                  if {@code millis} is {@code null}
     */
    public void setValue(BigDecimal millis, Integer localOffset);


    /**
     * Sets this timestamp to represent the point in time that is
     * {@code millis} milliseconds after 1970-01-01T00:00:00Z, with
     * the specified local offset of {@code localOffset}.
     *
     * @param millis
     *                  the number of milliseconds since 1970-01-01T00:00:00Z to
     *                  be represented by this timestamp.
     * @param localOffset
     *                  the local offset of this timestamp, in minutes. Zero
     *                  indicates UTC. {@code null} indicates the unknown
     *                  offset ({@code -00:00}).
     *
     * @throws IllegalArgumentException
     *                  if the resulting timestamp would precede
     *                  0001-01-01T00:00:00Z.
     */
    public void setValue(long millis, Integer localOffset);


    /**
     * Sets this timestamp to represent the point in time that is
     * <code>millis</code> milliseconds after 1970-01-01T00:00:00Z, with the
     * same local offset part.
     * <p>
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
     * <code>millis</code> milliseconds after 1970-01-01T00:00:00Z, with the
     * same local offset part.
     * <p>
     * If this is <code>null.timestamp</code>, then the local offset will be
     * unknown.
     *
     * @param millis the number of milliseconds since 1970-01-01T00:00:00Z to
     * be represented by this timestamp.
     * @throws IllegalArgumentException if the resulting timestamp would
     * precede 0001-01-01T00:00:00Z.
     * @throws NullPointerException if {@code millis} is {@code null}
     */
    public void setDecimalMillis(BigDecimal millis);


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

    // TODO add setCurrentTimeLocal();

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


    /**
     * Sets this timestamp to Ion <code>null.timestamp</code>.
     */
    public void makeNull();

    public IonTimestamp clone()
        throws UnknownSymbolException;
}
