// Copyright (c) 2008-2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.util.IonTextUtils.printCodePointAsString;

import com.amazon.ion.impl._Private_Utils;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * An <b>immutable</b> representation of time. Ion defines a simple
 * representation of date and time that is in Coordinated Universal Time (UTC).
 * In reality the practical use of time could be more accurately described of
 * UTC-SLS (UTC Smoothed Leap Seconds) as there is no representation for the
 * 24 (as of February 2009) leap second discontinuities that UTC has added.
 * <p>
 * This implementation preserves the "signficant digits" of the value.  The
 * precision defines which fields have been included.  Only common break
 * points in the values are supported.  Any unspecified fields are handled
 * as the start of the new year/month/day.
 *
 *
 * <h4>Equality and Comparison</h4>
 *
 * As with {@link IonValue} classes, the {@link #equals} methods on this class
 * performs a strict equivalence that observes the precision and local offset
 * of each timestamp.
 * This means that it's possible to have two {@link Timestamp} instances that
 * represent the same point in time but are not equivalent.
 * <p>
 * On the other hand, the {@link #compareTo} methods perform point in time
 * comparison, ignoring precision and local offset.
 * Thus the <em>natural comparison method</em> of this class is <em>not
 * consistent with equals</em>. See the documentation of {@link Comparable} for
 * further discussion.
 * <p>
 * To illustrate this distinction, consider the following timestamps. None are
 * {@link #equals} to each other, but any pair will return a zero result from
 * {@link #compareTo}.
 * <ul>
 *   <li>{@code 2009T}</li>
 *   <li>{@code 2009-01T}</li>
 *   <li>{@code 2009-01-01T}</li>
 *   <li>{@code 2009-01-01T00:00Z}</li>
 *   <li>{@code 2009-01-01T00:00:00Z}</li>
 *   <li>{@code 2009-01-01T00:00:00.0Z}</li>
 *   <li>{@code 2009-01-01T00:00:00.00Z}</li>
 *   <li>{@code 2009-01-01T00:00:00.000Z} <em>etc.</em></li>
 * </ul>
 *
 * @see #equals(Timestamp)
 * @see #compareTo(Timestamp)
 */
public final class Timestamp
    implements Comparable<Timestamp>, Cloneable
{
    /**
     * Unknown local offset from UTC.
     */
    public static final Integer UNKNOWN_OFFSET = null;

    /**
     * Local offset of zero hours from UTC.
     */
    public static final Integer UTC_OFFSET = Integer.valueOf(0);

    /** Used to check limits of decimal seconds. */
    private static final BigDecimal SIXTY = BigDecimal.valueOf(60);


    /**
     * The precision of the Timestamp.
     */
    public static enum Precision {
        YEAR,
        MONTH,
        DAY,
        MINUTE,
        SECOND,
        FRACTION
    }

    private static final int HASH_SIGNATURE =
        "INTERNAL TIMESTAMP".hashCode();

    /**
     * The precision of the Timestamp. The fractional seconds component is
     * defined by a BigDecimal.
     * <p>
     * During construction of all Timestamps, they will have a
     * date value (i.e. Year, Month, Day) but with reduced precision they may
     * exclude any time values that are more precise than the precision that is
     * being defined.
     */
    private Precision   _precision;

    // These are the time field values for the Timestamp.
    // _month and _day are 1-based (0 is an invalid value for
    // these in a non-null Timestamp).
    // TODO ION-328 - Represent internal time field values in its local time,
    // instead of UTC. This makes it much less confusing.
    private short       _year;
    private byte        _month = 1; // Initialized to valid default
    private byte        _day   = 1; // Initialized to valid default
    private byte        _hour;
    private byte        _minute;
    private byte        _second;
    private BigDecimal  _fraction;  // fractional seconds, must be within range [0, 1)

    /**
     * Minutes offset from UTC; zero means UTC proper,
     * <code>null</code> means that the offset is unknown.
     */
    private Integer     _offset;

                                                      //   jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec
                                                      // the first 0 is to make these arrays 1 based (since month values are 1-12)
    private static final int[] LEAP_DAYS_IN_MONTH   = { 0,  31,  29,  31,  30,  31,  30,  31,  31,  30,  31,  30,  31 };
    private static final int[] NORMAL_DAYS_IN_MONTH = { 0,  31,  28,  31,  30,  31,  30,  31,  31,  30,  31,  30,  31 };

    private static int last_day_in_month(int year, int month) {
        boolean is_leap;
        if ((year % 4) == 0) {
            // divisible by 4 (lower 2 bits are zero) - may be a leap year
            if ((year % 100) == 0) {
                // and divisible by 100 - not a leap year
                if ((year % 400) == 0) {
                    // but divisible by 400 - then it is a leap year
                    is_leap = true;
                }
                else {
                    is_leap = false;
                }
            }
            else {
                is_leap = true;
            }
        }
        else {
            is_leap = false;
        }
        return is_leap ? LEAP_DAYS_IN_MONTH[month] : NORMAL_DAYS_IN_MONTH[month];
    }

    /**
     * Applies the local time zone offset from UTC to the applicable time field
     * values. Depending on the local time zone offset, adjustments
     * (i.e. rollover) will be made to the Year, Day, Hour, Minute time field
     * values.
     *
     * @param offset the local offset, in minutes from UTC.
     */
    private void apply_offset(int offset)
    {
        if (offset == 0) return;
        if (offset < -24*60 || offset > 24*60) {
            throw new IllegalArgumentException("bad offset " + offset);
        }
        // To convert _to_ UTC you must SUBTRACT the local offset
        offset = -offset;
        int hour_offset = offset / 60;
        int min_offset = offset - (hour_offset * 60);
        if (offset < 0) {
            _minute += min_offset;  // lower the minute value by adding a negative offset
            _hour += hour_offset;
            if (_minute < 0) {
                _minute += 60;
                _hour -= 1;
            }
            if (_hour >= 0) return;  // hour is 0-23
            _hour += 24;
            _day -= 1;
            if (_day >= 1) return;  // day is 1-31
            // we can't do this until we've figured out the month and year: _day += last_day_in_month(_year, _month);
            _month -= 1;
            if (_month >= 1) {
                _day += last_day_in_month(_year, _month);  // now we know (when the year doesn't change
                assert(_day == last_day_in_month(_year, _month));
                return;  // 1-12
            }
            _month += 12;
            _year -= 1;
            if (_year < 1) throw new IllegalArgumentException("year is less than 1");
            _day += last_day_in_month(_year, _month);  // and now we know, even if the year did change
            assert(_day == last_day_in_month(_year, _month));
        }
        else {
            _minute += min_offset;  // lower the minute value by adding a negative offset
            _hour += hour_offset;
            if (_minute > 59) {
                _minute -= 60;
                _hour += 1;
            }
            if (_hour < 24) return;  // hour is 0-23
            _hour -= 24;
            _day += 1;
            if (_day <= last_day_in_month(_year, _month)) return;  // day is 1-31
            // we can't do this until we figure out the final month and year: _day -= last_day_in_month(_year, _month);
            _day = 1; // this is always the case
            _month += 1;
            if (_month <= 12) {
                return;  // 1-12
            }
            _month -= 12;
            _year += 1;
            if (_year > 9999) throw new IllegalArgumentException("year exceeds 9999");
        }
    }

    /**
     * This method uses deprecated methods from {@link java.util.Date}
     * instead of {@link Calendar} so that this code can be used (more easily)
     * on the mobile Java platform (which has Date but does not have Calendar).
     */
    @SuppressWarnings("deprecation")
    private void set_fields_from_millis(Date date) {
        this._year    = (short)(date.getYear() + 1900);
        this._month   = (byte)(date.getMonth() + 1);  // calendar months are 0 based, timestamp months are 1 based
        this._day     = (byte)date.getDate();
        this._hour    = (byte)date.getHours();
        this._minute  = (byte)date.getMinutes();
        this._second  = (byte)date.getSeconds();
        // this is done because the y-m-d values are in the local timezone
        // so this adjusts the value back to zulu time (UTC)
        // Note that the sign on this is opposite of Ion (and Calendar) offset.
        // Example: PST = 480 here but Ion/Calendar use -480 = -08:00 = UTC-8
        int offset = date.getTimezoneOffset();
        this.apply_offset(-offset);
    }

    /**
     * Copies data from a {@link Calendar} into this timestamp.
     * Must only be called during construction due to timestamp immutabliity.
     *
     * @param cal must have at least one field set.
     *
     * @throws IllegalArgumentException if the calendar has no fields set.
     */
    private void set_fields_from_calendar(Calendar cal)
    {
        if (cal.isSet(Calendar.MILLISECOND)) {
            this._precision = Precision.FRACTION;
        }
        else if (cal.isSet(Calendar.SECOND)) {
             this._precision = Precision.SECOND;
        }
        else if (cal.isSet(Calendar.HOUR_OF_DAY) || cal.isSet(Calendar.MINUTE)) {
            this._precision = Precision.MINUTE;
        }
        else if (cal.isSet(Calendar.DAY_OF_MONTH)) {
            this._precision = Precision.DAY;
       }
        else if (cal.isSet(Calendar.MONTH)) {
            this._precision = Precision.MONTH;
       }
        else if (cal.isSet(Calendar.YEAR)) {
            this._precision = Precision.YEAR;
        }
        else {
            throw new IllegalArgumentException("Calendar has no fields set");
        }

        switch (this._precision) {
            case FRACTION:
                BigDecimal millis = BigDecimal.valueOf(cal.get(Calendar.MILLISECOND));
                this._fraction = millis.movePointLeft(3); // convert to fraction
            case SECOND:
                this._second = (byte)cal.get(Calendar.SECOND);
            case MINUTE:
                this._hour   = (byte)cal.get(Calendar.HOUR_OF_DAY);
                this._minute = (byte)cal.get(Calendar.MINUTE);
            case DAY:
                this._day    = (byte)cal.get(Calendar.DAY_OF_MONTH);
            case MONTH:
                // Calendar months are 0 based, Timestamp months are 1 based
                this._month  = (byte)(cal.get(Calendar.MONTH) + 1);
            case YEAR:
                this._year   = (short)cal.get(Calendar.YEAR);
        }

        // Strangely, if this test is made before calling get(), it will return
        // false even when Calendar.setTimeZone() was called.
        if (cal.isSet(Calendar.ZONE_OFFSET)) {
            int offset = cal.get(Calendar.ZONE_OFFSET);
            if (cal.isSet(Calendar.DST_OFFSET)) {
                offset += cal.get(Calendar.DST_OFFSET);
            }

            // convert ms to minutes
            _offset = offset / (1000*60);
            // Transform our members from local time to Zulu
            this.apply_offset(_offset);
        }
    }


    private void validate_fields()
    {
        if (_year < 1  || _year > 9999) error_in_field("year must be between 1 and 9999 inclusive UTC, and local time");
        if (_month < 1 || _month > 12) error_in_field("month is between 1 and 12 inclusive");
        if (_day < 1   || _day > last_day_in_month(_year, _month)) {
            error_in_field("Day in month " + _year + "-" + _month
                           + " must between 1 and "
                           + last_day_in_month(_year, _month) + " inclusive");
        }

        if (_hour < 0 || _hour > 23)     error_in_field("hour is between 0 and 23 inclusive");
        if (_minute < 0 || _minute > 59) error_in_field("minute is between 0 and 59 inclusive");
        if (_second < 0 || _second > 59) error_in_field("second is between 0 and 59 inclusive");

        if (this._precision == Precision.FRACTION) {
            if (_fraction == null) error_in_field("fractional seconds cannot be null when the precision is Timestamp.TT_FRAC");
            if (_fraction.signum() == -1) {
                error_in_field("fractional seconds must be greater than or equal to 0 and less than 1");
            }
            if (BigDecimal.ONE.compareTo(_fraction) != 1) {
                error_in_field("fractional seconds must be greater than or equal to 0 and less than 1");
            }
        }
    }

    private static void error_in_field(String message)
    {
        IllegalArgumentException e = new IllegalArgumentException(message);
        throw e;
    }

    /**
     * Creates a new Timestamp, precise to the day, with unknown local offset.
     * <p>
     * This is equivalent to the corresponding Ion value {@code YYYY-MM-DD}.
     *
     * @deprecated Since IonJava R17.
     * Use {@link #forDay(int, int, int)} instead.
     */
    @Deprecated
    public Timestamp(int zyear, int zmonth, int zday)
    {
        if (zyear < 1 || zyear > 9999) throw new IllegalArgumentException("year is between 1 and 9999 inclusive");
        if (zmonth < 1 || zmonth > 12) throw new IllegalArgumentException("month is between 1 and 12 inclusive");
        int end_of_month = last_day_in_month(zyear, zmonth);
        if (zday < 1 || zday > end_of_month) throw new IllegalArgumentException("day is between 1 and "+end_of_month+" inclusive");
        this._precision = Precision.DAY;
        this._day       = (byte)zday;  // days are base 1 (as you'd expect)
        this._month     = (byte)zmonth;
        this._year      = (short)zyear;
        validate_fields();
        this._offset    = UNKNOWN_OFFSET;
    }


    /**
     * Creates a new Timestamp, precise to the minute, with a given local
     * offset.
     * <p>
     * This is equivalent to the corresponding Ion value
     * {@code YYYY-MM-DDThh:mm+-oo:oo}, where {@code oo:oo} represents the
     * hour and minutes of the local offset from UTC.
     *
     * @param offset
     *          the local offset from UTC, measured in minutes;
     *          may be {@code null} to represent an unknown local offset
     *
     * @deprecated Since IonJava R17.
     * Use {@link #forMinute(int, int, int, int, int, Integer)} instead.
     */
    @Deprecated
    public Timestamp(int year, int month, int day,
                     int hour, int minute,
                     Integer offset)
    {
        this._precision = Precision.MINUTE;
        this._minute    = (byte)minute;
        this._hour      = (byte)hour;
        this._day       = (byte)day;  // days are base 1 (as you'd expect)
        this._month     = (byte)month;
        this._year      = (short)year;

        validate_fields();
        if (offset != null) {
            this._offset = offset;
            apply_offset(offset);
        }
    }

    /**
     * Creates a new Timestamp, precise to the second, with a given local
     * offset.
     * <p>
     * This is equivalent to the corresponding Ion value
     * {@code YYYY-MM-DDThh:mm:ss+-oo:oo}, where {@code oo:oo} represents the
     * hour and minutes of the local offset from UTC.
     *
     * @param offset
     *          the local offset from UTC, measured in minutes;
     *          may be {@code null} to represent an unknown local offset.
     *
     * @deprecated Since IonJava R17.
     * Use {@link #forSecond(int, int, int, int, int, int, Integer)} instead.
     */
    @Deprecated
    public Timestamp(int year, int month, int day,
                     int hour, int minute, int second,
                     Integer offset)
    {
        this._precision = Precision.SECOND;
        this._second    = (byte)second;
        this._minute    = (byte)minute;
        this._hour      = (byte)hour;
        this._day       = (byte)day;  // days are base 1 (as you'd expect)
        this._month     = (byte)month;
        this._year      = (short)year;

        validate_fields();
        if (offset != null) {
            this._offset = offset;
            apply_offset(offset);
        }
    }

    /**
     * Creates a new Timestamp, precise to the second or fractional second,
     * with a given local offset.
     * <p>
     * This is equivalent to the corresponding Ion value
     * {@code YYYY-MM-DDThh:mm:ss.fff+-oo:oo}, where {@code oo:oo} represents
     * the hour and minutes of the local offset from UTC, and {@code fff}
     * represents the fractional seconds.
     *
     * @param frac
     *          the fractional seconds; must not be {@code null}; if negative,
     *          its absolute value is used
     * @param offset
     *          the local offset from UTC, measured in minutes;
     *          may be {@code null} to represent an unknown local offset
     *
     * @throws NullPointerException if {@code frac} is {@code null}
     *
     * @deprecated Since IonJava R17.
     * Use {@link #forSecond(int, int, int, int, int, BigDecimal, Integer)}
     * instead.
     */
    @Deprecated
    public Timestamp(int year, int month, int day,
                     int hour, int minute, int second, BigDecimal frac,
                     Integer offset)
    {
        // When there's no fraction, use SECOND precision.  Otherwise, our
        // comparisons will fail versus other constructors with equivalent
        // data.
        if (frac.equals(BigDecimal.ZERO))
        {
            _precision = Precision.SECOND;
            _fraction = null;
        }
        else
        {
            _precision = Precision.FRACTION;
            _fraction = frac.abs();
        }
        _second   = (byte)second;
        _minute   = (byte)minute;
        _hour     = (byte)hour;
        _day      = (byte)day;  // days are base 1 (as you'd expect)
        _month    = (byte)month;
        _year     = (short)year;

        validate_fields();
        if (offset != null) {
            this._offset = offset;
            apply_offset(offset);
        }
    }

    /**
     * Creates a new Timestamp from the individual time components. The
     * individual time components are expected to be in UTC,
     * with the local offset from UTC (i.e. {@code offset}) <em>already
     * applied</em> to the time components.
     * <p>
     * Any time component that is more precise
     * than the precision parameter {@code p} will be <em>excluded</em> from the
     * calculation of the resulting Timestamp's point in time.
     *
     * @param offset
     *          the local offset from UTC, measured in minutes;
     *          may be {@code null} to represent an unknown local offset
     *
     * @see #createFromUtcFields(Precision, int, int, int, int, int, int, BigDecimal, Integer)
     */
    private Timestamp(Precision p, int zyear, int zmonth, int zday,
                      int zhour, int zminute, int zsecond, BigDecimal frac,
                      Integer offset)
    {
        _precision = p;
        switch (p) {
        default:
            throw new IllegalArgumentException("invalid Precision passed to constructor");
        case FRACTION:
            if (frac.equals(BigDecimal.ZERO))
            {
                _precision = Precision.SECOND;
                _fraction = null;
            }
            else
            {
                _fraction = frac.abs();
            }
        case SECOND:
            _second = (byte)zsecond;
        case MINUTE:
            _minute = (byte)zminute;
            _hour   = (byte)zhour;
        case DAY:
            _day    = (byte)zday;  // days are base 1 (as you'd expect)
        case MONTH:
            _month  = (byte)zmonth;
        case YEAR:
            _year   = (short)zyear;
        }
        validate_fields();
        _offset = offset;
        // This doesn't call applyOffset() like the other constructors because
        // we already expect the time parameters to be in UTC, and that's
        // already what we're supposed to have.
    }

    /**
     * Creates a new Timestamp from the individual time components. The
     * individual time components are expected to be in UTC,
     * with the local offset from UTC (i.e. {@code offset}) <em>already
     * applied</em> to the time components.
     * As such, if the given {@code offset} is non-null or zero, the resulting
     * Timestamp will have time values that <em>DO NOT</em> match the time
     * parameters. This method also has a behavior of precision "narrowing",
     * detailed in the sub-section below.
     *
     * <p>
     * For example, the following method calls will return Timestamps with
     * values (in its local time) respectively:
     * <pre>
     * createFromUtcFields(Precision.FRACTION, 2012, 2, 3, 4, 5, 6, 0.007, <b>null</b>)    will return 2012-02-03T04:05:06.007-00:00 (match)
     * createFromUtcFields(Precision.FRACTION, 2012, 2, 3, 4, 5, 6, 0.007, <b>0</b>)       will return 2012-02-03T04:05:06.007+00:00 (match)
     * createFromUtcFields(Precision.FRACTION, 2012, 2, 3, 4, 5, 6, 0.007, <b>480</b>)     will return 2012-02-03T<b>12</b>:05:06.007<b>+08:00</b> (do not match)
     * createFromUtcFields(Precision.FRACTION, 2012, 2, 3, 4, 5, 6, 0.007, <b>-480</b>)    will return 2012-02-<b>02</b>T<b>20</b>:05:06.007<b>-08:00</b> (do not match)
     * createFromUtcFields(Precision.FRACTION, 2012, 2, 3, 4, 5, 6, 0.007, <b>720</b>)     will return 2012-02-03T<b>16</b>:05:06.007<b>+12:00</b> (do not match)
     * createFromUtcFields(Precision.FRACTION, 2012, 2, 3, 4, 5, 6, 0.007, <b>-720</b>)    will return 2012-02-<b>02</b>T<b>16</b>:05:06.007<b>-12:00</b> (do not match)
     *
     * Note: All of these resulting Timestamps have the similar value (in UTC) 2012-02-03T04:05:06.007Z.
     * </pre>
     *
     *
     * <h4>Precision "Narrowing"</h4>
     *
     * <p>
     * Any time component that is more precise
     * than the precision parameter {@code p} will be <em>excluded</em> from the
     * calculation of the resulting Timestamp's point in time.
     * <p>
     * For example, the following method calls will return Timestamps with
     * values respectively:
     * <pre>
     * createFromUtcFields(<b>Precision.YEAR</b>    , 2012, 2, 3, 4, 5, 6, 0.007, 0)    will return 2012T
     * createFromUtcFields(<b>Precision.MONTH</b>   , 2012, 2, 3, 4, 5, 6, 0.007, 0)    will return 2012-02T
     * createFromUtcFields(<b>Precision.DAY</b>     , 2012, 2, 3, 4, 5, 6, 0.007, 0)    will return 2012-02-03T
     * createFromUtcFields(<b>Precision.MINUTE</b>  , 2012, 2, 3, 4, 5, 6, 0.007, 0)    will return 2012-02-03T04:05Z
     * createFromUtcFields(<b>Precision.SECOND</b>  , 2012, 2, 3, 4, 5, 6, 0.007, 0)    will return 2012-02-03T04:05:06Z
     * createFromUtcFields(<b>Precision.FRACTION</b>, 2012, 2, 3, 4, 5, 6, 0.007, 0)    will return 2012-02-03T04:05:06.007Z
     * </pre>
     *
     * @param p the desired timestamp precision. The result may have a
     * different precision if the input data isn't precise enough.
     *
     * @param offset
     *          the local offset from UTC, measured in minutes;
     *          may be {@code null} to represent an unknown local offset.
     *
     * @deprecated Since IonJava R17.
     */
    @Deprecated
    public static Timestamp
    createFromUtcFields(Precision p, int zyear, int zmonth, int zday,
                        int zhour, int zminute, int zsecond, BigDecimal frac,
                        Integer offset)
    {
        return new Timestamp(p, zyear, zmonth, zday,
                             zhour, zminute, zsecond, frac,
                             offset);
    }

    /**
     * Creates a new Timestamp from a {@link Calendar}, preserving the
     * {@link Calendar}'s precision and local offset from UTC.
     * <p>
     * The most precise calendar field of {@code cal} will be used to determine
     * the precision of the resulting Timestamp.
     *
     * For example, the calendar field will have a Timestamp precision accordingly:
     * <ul>
     *   <li>{@link Calendar#YEAR} - year precision</li>
     *   <li>{@link Calendar#MONTH} - month precision</li>
     *   <li>{@link Calendar#DAY_OF_MONTH} - day precision</li>
     *   <li>{@link Calendar#HOUR_OF_DAY} or {@link Calendar#MINUTE} - minute precision</li>
     *   <li>{@link Calendar#SECOND} - second precision</li>
     *   <li>{@link Calendar#MILLISECOND} - fractional second precision</li>
     * </ul>
     *
     * @throws IllegalArgumentException
     *          if {@code cal} has no appropriate calendar fields set.
     *
     * @deprecated Since IonJava R17.
     * Use {@link #forCalendar(Calendar)} instead.
     */
    @Deprecated
    public Timestamp(Calendar cal)
    {
        set_fields_from_calendar(cal);
    }

    /**
     * Creates a new Timestamp that represents the point in time that is
     * {@code millis} milliseconds (including any fractional
     * milliseconds) from the epoch, with a given local offset.
     *
     * <p>
     * The resulting Timestamp will be precise to the second if {@code millis}
     * doesn't contain information that is more granular than seconds.
     * For example, a {@code BigDecimal} of
     * value <tt>132541995e4 (132541995 &times; 10<sup>4</sup>)</tt>
     * will return a Timestamp of {@code 2012-01-01T12:12:30Z},
     * precise to the second.
     *
     * <p>
     * The resulting Timestamp will be precise to the fractional second if
     * {@code millis} contains information that is at least granular to
     * milliseconds.
     * For example, a {@code BigDecimal} of
     * value <tt>1325419950555</tt>
     * will return a Timestamp of {@code 2012-01-01T12:12:30.555Z},
     * precise to the fractional second.
     *
     * @param millis
     *          number of milliseconds (including any fractional
     *          milliseconds) from the epoch (1970-01-01T00:00:00.000Z);
     *          must not be {@code null}
     * @param localOffset
     *          the local offset from UTC, measured in minutes;
     *          may be {@code null} to represent an unknown local offset
     *
     * @throws NullPointerException if {@code millis} is {@code null}
     *
     * @deprecated Since IonJava R17.
     * Use {@link #forMillis(BigDecimal, Integer)} instead.
     */
    @Deprecated
    public Timestamp(BigDecimal millis, Integer localOffset)
    {
        if (millis == null) throw new NullPointerException("millis is null");

        long ms = millis.longValue();
        Date date = new Date(ms);
        set_fields_from_millis(date);

        int scale = millis.scale();
        if (scale < -3) {
            this._precision = Precision.SECOND;
            this._fraction = null;
        }
        else {
            this._precision = Precision.FRACTION;
            BigDecimal secs = millis.movePointLeft(3);
            BigDecimal secsDown = secs.setScale(0, RoundingMode.FLOOR);
            this._fraction = secs.subtract(secsDown);
        }
        this._offset = localOffset;
        this.validate_fields();
    }

    /**
     * Creates a new Timestamp that represents the point in time that is
     * {@code millis} milliseconds from the epoch, with a given local offset.
     * <p>
     * The resulting Timestamp will be precise to the fractional second.
     *
     * @param millis
     *          number of milliseconds from the epoch (1970-01-01T00:00:00.000Z)
     * @param localOffset
     *          the local offset from UTC, measured in minutes;
     *          may be {@code null} to represent an unknown local offset.
     *
     * @deprecated Since IonJava R17.
     * Use {@link #forMillis(long, Integer)} instead.
     */
    @Deprecated
    public Timestamp(long millis, Integer localOffset)
    {
        Date d = new Date(millis);  // this will have the system timezone in it whether we want it or not
        this.set_fields_from_millis(d);

        // fractional seconds portion
        this._precision = Precision.FRACTION;
        BigDecimal secs = BigDecimal.valueOf(millis).movePointLeft(3);
        BigDecimal secsDown = secs.setScale(0, RoundingMode.FLOOR);
        this._fraction = secs.subtract(secsDown);

        this._offset = localOffset;
        this.validate_fields();
    }

    /**
     * Returns a new Timestamp that represents the point in time, precision
     * and local offset defined in Ion format by the {@link CharSequence}.
     *
     * @param ionFormattedTimestamp
     *          a sequence of characters that is the Ion representation of a
     *          Timestamp
     *
     * @throws IllegalArgumentException
     *          if the {@code CharSequence} is an invalid Ion representation
     *          of a Timestamp;
     *          or if the {@code CharSequence} has excess characters which
     *          are not one of the following valid thirteen numeric-stop
     *          characters (escaped accordingly for readability):
     *          <code>{}[](),\"\'\ \t\n\r}</code>
     *
     * @return
     *          {@code null} if the {@code CharSequence} is "null.timestamp"
     *
     * @see <a href="https://w.amazon.com/index.php/Ion#Timestamps">Ion Timestamp Wiki</a>
     * @see <a href="http://www.w3.org/TR/NOTE-datetime">W3C Note on Date and Time Formats</a>
     */
    public static Timestamp valueOf(CharSequence ionFormattedTimestamp) {
        final String NULL_TIMESTAMP_IMAGE = "null.timestamp";
        final int    LEN_OF_NULL_IMAGE    = NULL_TIMESTAMP_IMAGE.length();
        final int    END_OF_YEAR          =  4;  // 1234T
        final int    END_OF_MONTH         =  7;  // 1234-67T
        final int    END_OF_DAY           = 10;  // 1234-67-90T
        final int    END_OF_MINUTES       = 16;
        final int    END_OF_SECONDS       = 19;

        int temp, pos;
        int length = -1;

        if (ionFormattedTimestamp == null || ionFormattedTimestamp.length() < 1) {
            throw new IllegalArgumentException("empty timestamp");
        }
        length = ionFormattedTimestamp.length();

        // check for 'null.timestamp'
        if (ionFormattedTimestamp.charAt(0) == 'n') {
            if (length >= LEN_OF_NULL_IMAGE
                && NULL_TIMESTAMP_IMAGE.equals(ionFormattedTimestamp.subSequence(0, LEN_OF_NULL_IMAGE).toString()))
            {
                if (length > LEN_OF_NULL_IMAGE) {
                    if (!isValidFollowChar(ionFormattedTimestamp.charAt(LEN_OF_NULL_IMAGE))) {
                        throw new IllegalArgumentException("invalid timestamp: " + ionFormattedTimestamp);
                    }
                }
                return null;
            }
            throw new IllegalArgumentException("invalid timestamp: " + ionFormattedTimestamp);
        }

        int year  = 1;
        int month = 1;
        int day   = 1;
        int hour  = 0;
        int minute = 0;
        int seconds = 0;
        BigDecimal fraction = null;
        Precision precision;

        // fake label to turn goto's into a break so Java is happy :) enjoy
        do {
            // otherwise we expect yyyy-mm-ddThh:mm:ss.ssss+hh:mm
            if (length < END_OF_YEAR + 1) {  // +1 for the "T"
                throw new IllegalArgumentException("invalid timestamp image: way too short (must be at least yyyyT)");
            }
            pos = END_OF_YEAR;
            precision = Precision.YEAR;
            year  = read_digits(ionFormattedTimestamp, 0, 4, -1, "year");

            char c = ionFormattedTimestamp.charAt(END_OF_YEAR);
            if (c == 'T') break;
            if (c != '-') {
                throw new IllegalArgumentException("invalid timestamp: expected \"-\" between year and month, found " + printCodePointAsString(c));
            }
            if (length < END_OF_MONTH + 1) {  // +1 for the "T"
                throw new IllegalArgumentException("invalid timestamp image: year month form is too short (must be yyyy-mmT)");
            }
            pos = END_OF_MONTH;
            precision = Precision.MONTH;
            month = read_digits(ionFormattedTimestamp, END_OF_YEAR + 1, 2, -1, "month");

            c = ionFormattedTimestamp.charAt(END_OF_MONTH);
            if (c == 'T') break;
            if (c != '-') {
                throw new IllegalArgumentException("invalid timestamp: expected \"-\" between month and day, found " + printCodePointAsString(c));
            }
            if (length < END_OF_DAY) {
                throw new IllegalArgumentException("invalid timestamp image: too short for yyyy-mm-dd");
            }
            pos = END_OF_DAY;
            precision = Precision.DAY;
            day   = read_digits(ionFormattedTimestamp, END_OF_MONTH + 1, 2, -1, "day");
            if (length == END_OF_DAY) break;
            c = ionFormattedTimestamp.charAt(END_OF_DAY);
            if (c != 'T') {
                throw new IllegalArgumentException("invalid timestamp: expected \"T\" after day, found " + printCodePointAsString(c));
            }
            if (length == END_OF_DAY + 1) break;

            // now lets see if we have a time value
            if (length < END_OF_MINUTES) {
                throw new IllegalArgumentException("invalid timestamp image: too short for yyyy-mm-ddThh:mm");
            }
            hour   = read_digits(ionFormattedTimestamp, 11, 2, ':', "hour");
            minute = read_digits(ionFormattedTimestamp, 14, 2, -1, "minutes");
            pos = END_OF_MINUTES;
            precision = Precision.MINUTE;

            // we may have seconds
            if (length <= END_OF_MINUTES || ionFormattedTimestamp.charAt(END_OF_MINUTES) != ':') break;
            if (length < END_OF_SECONDS) {
                throw new IllegalArgumentException("invalid timestamp imagetoo short for yyyy-mm-ddThh:mm:ss");
            }
            seconds = read_digits(ionFormattedTimestamp, 17, 2, -1, "seconds");
            pos = END_OF_SECONDS;
            precision = Precision.SECOND;

            if (length <= END_OF_SECONDS || ionFormattedTimestamp.charAt(END_OF_SECONDS) != '.') break;
            precision = Precision.SECOND;
            pos = END_OF_SECONDS + 1;
            while (length > pos && Character.isDigit(ionFormattedTimestamp.charAt(pos))) {
                pos++;
            }
            if (pos <= END_OF_SECONDS + 1) {
                throw new IllegalArgumentException("Timestamp must have at least one digit after decimal point: " + ionFormattedTimestamp);
            }
            precision = Precision.FRACTION;
            fraction = new BigDecimal(ionFormattedTimestamp.subSequence(19, pos).toString());
        } while (false);

        Integer offset;

        // now see if they included a timezone offset
        char timezone_start = pos < length ? ionFormattedTimestamp.charAt(pos) : '\n';
        if (timezone_start == 'Z') {
            offset = 0;
            pos++;
        }
        else if (timezone_start == '+' || timezone_start == '-') {
            if (length < pos + 5) {
                throw new IllegalArgumentException("invalid timestamp image: timezone too short");
            }
            // +/- hh:mm
            pos++;
            temp = read_digits(ionFormattedTimestamp, pos, 2, ':', "local offset hours");
            pos += 3;
            temp = temp * 60 + read_digits(ionFormattedTimestamp, pos, 2, -1, "local offset minutes");
            pos += 2;
            if (temp >= 24*60) throw new IllegalArgumentException("invalid timezone offset: timezone offset must not be more than 1 day");
            if (timezone_start == '-') {
                temp = -temp;
            }
            if (temp == 0 && timezone_start == '-') {
                // int doesn't do negative zero very elegantly
                offset = null;
            }
            else {
                offset = temp;
            }
        }
        else {
            switch (precision) {
                case YEAR:
                case MONTH:
                case DAY:
                    break;
                default:
                    error_in_field("Timestamp must have local offset: " + ionFormattedTimestamp);
            }
            offset = null;
        }
        if (ionFormattedTimestamp.length() > (pos + 1) && !isValidFollowChar(ionFormattedTimestamp.charAt(pos + 1))) {
            error_in_field("invalid excess characters encountered");
        }

        Timestamp ts =
            new Timestamp(precision, year, month, day,
                          hour, minute, seconds, fraction, offset);
        if (offset != null) {
            // if there is a local offset, we have to adjust the date/time value
            ts.apply_offset(offset);
        }
        return ts;
    }

    private static int read_digits(CharSequence in, int start, int length,
                                   int terminator, String field)
    {
        int ii, value = 0;
        int end = start + length;

        if (in.length() < end) {
            error_in_field("Malformed timestamp, " + field
                           + " requires " + length + " digits: " + in);
        }

        for (ii=start; ii<end; ii++) {
            char c = in.charAt(ii);
            if (!Character.isDigit(c)) {
                // FIXME this will give incorrect message if c is a surrogate
                error_in_field("Malformed timestamp, " + field
                               + " has non-digit character "
                               + printCodePointAsString(c)
                               + "\": " + in);
            }
            value *= 10;
            value += c - '0';
        }

        // Check the terminator if requested.
        if (terminator != -1) {
            if (ii >= in.length() || in.charAt(ii) != terminator) {
                error_in_field("Malformed timestamp, " + field
                               + " should end with "
                               + printCodePointAsString(terminator)
                               + "\": " + in);
            }
        }
        // Otherwise make sure we don't have too many digits.
        else if (ii < in.length() && Character.isDigit(in.charAt(ii))) {
            error_in_field("Malformed timestamp, " + field
                           + " requires " + length + " digits but has more: "
                           + in);
        }

        return value;
    }

    private static boolean isValidFollowChar(char c) {
        switch (c) {
        default:
            return false;
        case '{':
        case '}':
        case '[':
        case ']':
        case '(':
        case ')':
        case ',':
        case '\"':
        case '\'':
        case '\\':
        case '\t':
        case '\n':
        case '\r':
            return true;
        }
    }

    @Override
    public Timestamp clone()
    {
        // The Copy-Constructor we're using here already expects the time field
        // values to be in UTC, and that is already what we have for this
        // Timestamp -- no adjustment necessary to make it local time.
        return new Timestamp(_precision,
                             _year,
                             _month,
                             _day,
                             _hour,
                             _minute,
                             _second,
                             _fraction,
                             _offset);
    }

    /**
     * Applies the local offset from UTC to each of the applicable time field
     * values and returns the new Timestamp. In short, this makes the Timestamp
     * represent local time.
     *
     * @return a new Timestamp in its local time
     */
    private Timestamp make_localtime()
    {
        int offset = _offset != null
            ? _offset.intValue()
            : 0;

        // We use a Copy-Constructor that expects the time parameters to be in
        // UTC, as that's what we're supposed to have.
        // As this Copy-Constructor doesn't apply local offset to the time
        // field values (it assumes that the local offset is already applied to
        // them), we explicitly apply the local offset to the time field values
        // after we obtain the new Timestamp instance.
        Timestamp localtime = new Timestamp(_precision,
                                            _year,
                                            _month,
                                            _day,
                                            _hour,
                                            _minute,
                                            _second,
                                            _fraction,
                                            _offset);
        // explicitly apply the local offset to the time field values
        localtime.apply_offset(-offset);

        assert localtime._offset == _offset;

        return localtime;
    }


    /**
     * Returns a Timestamp, precise to the day, with unknown local offset.
     * <p>
     * This is equivalent to the corresponding Ion value {@code YYYY-MM-DD}.
     */
    public static Timestamp forDay(int yearZ, int monthZ, int dayZ)
    {
        return new Timestamp(yearZ, monthZ, dayZ);
    }


    /**
     * Returns a Timestamp, precise to the minute, with a given local
     * offset.
     * <p>
     * This is equivalent to the corresponding Ion value
     * {@code YYYY-MM-DDThh:mm+-oo:oo}, where {@code oo:oo} represents the
     * hour and minutes of the local offset from UTC.
     *
     * @param offset
     *          the local offset from UTC, measured in minutes;
     *          may be {@code null} to represent an unknown local offset
     */
    public static Timestamp forMinute(int year, int month, int day,
                                      int hour, int minute,
                                      Integer offset)
    {
        return new Timestamp(year, month, day, hour, minute, offset);
    }


    /**
     * Returns a Timestamp, precise to the second, with a given local offset.
     * <p>
     * This is equivalent to the corresponding Ion value
     * {@code YYYY-MM-DDThh:mm:ss+-oo:oo}, where {@code oo:oo} represents the
     * hour and minutes of the local offset from UTC.
     *
     * @param offset
     *          the local offset from UTC, measured in minutes;
     *          may be {@code null} to represent an unknown local offset
     */
    public static Timestamp forSecond(int year, int month, int day,
                                      int hour, int minute, int second,
                                      Integer offset)
    {
        return new Timestamp(year, month, day, hour, minute, second, offset);
    }


    /**
     * Returns a Timestamp, precise to the second, with a given local offset.
     * <p>
     * This is equivalent to the corresponding Ion value
     * {@code YYYY-MM-DDThh:mm:ss.sss+-oo:oo}, where {@code oo:oo} represents
     * the hour and minutes of the local offset from UTC.
     *
     * @param second must be at least zero and less than 60.
     * Must not be null.
     *
     * @param offset
     *          the local offset from UTC, measured in minutes;
     *          may be {@code null} to represent an unknown local offset
     */
    public static Timestamp forSecond(int year, int month, int day,
                                      int hour, int minute, BigDecimal second,
                                      Integer offset)
    {
        if (second.signum() < 0 || second.compareTo(SIXTY) >= 0)
        {
            error_in_field("second must be at least 0 and less than 60");
        }
        // Tease apart the whole and fractional seconds.
        // Storing them separately is silly.
        int s = second.intValue();
        BigDecimal frac = second.subtract(BigDecimal.valueOf(s));
        return new Timestamp(year, month, day, hour, minute, s, frac, offset);
    }


    /**
     * Returns a Timestamp that represents the point in time that is
     * {@code millis} milliseconds from the epoch, with a given local offset.
     * <p>
     * The resulting Timestamp will be precise to the millisecond.
     *
     * @param millis
     * the number of milliseconds from the epoch (1970-01-01T00:00:00.000Z).
     * @param localOffset
     *          the local offset from UTC, measured in minutes;
     *          may be {@code null} to represent an unknown local offset.
     */
    public static Timestamp forMillis(long millis, Integer localOffset)
    {
        return new Timestamp(millis, localOffset);
    }


    /**
     * Returns a Timestamp that represents the point in time that is
     * {@code millis} milliseconds (including any fractional
     * milliseconds) from the epoch, with a given local offset.
     *
     * <p>
     * The resulting Timestamp will be precise to the second if {@code millis}
     * doesn't contain information that is more granular than seconds.
     * For example, a {@code BigDecimal} of
     * value <tt>132541995e4 (132541995 &times; 10<sup>4</sup>)</tt>
     * will return a Timestamp of {@code 2012-01-01T12:12:30Z},
     * precise to the second.
     *
     * <p>
     * The resulting Timestamp will be precise to the fractional second if
     * {@code millis} contains information that is at least granular to
     * milliseconds.
     * For example, a {@code BigDecimal} of
     * value <tt>1325419950555</tt>
     * will return a Timestamp of {@code 2012-01-01T12:12:30.555Z},
     * precise to the fractional second.
     *
     * @param millis
     *          number of milliseconds (including any fractional
     *          milliseconds) from the epoch (1970-01-01T00:00:00.000Z);
     *          must not be {@code null}
     * @param localOffset
     *          the local offset from UTC, measured in minutes;
     *          may be {@code null} to represent an unknown local offset
     *
     * @throws NullPointerException if {@code millis} is {@code null}
     */
    public static Timestamp forMillis(BigDecimal millis, Integer localOffset)
    {
        return new Timestamp(millis, localOffset);
    }


    /**
     * Converts a {@link Calendar} to a Timestamp, preserving the calendar's
     * time zone as the equivalent local offset.
     *
     * @return a Timestamp instance, precise to the millisecond;
     *   or {@code null} if {@code calendar} is {@code null}
     */
    public static Timestamp forCalendar(Calendar calendar)
    {
        if (calendar == null) return null;
        return new Timestamp(calendar);
    }


    /**
     * Converts a {@link Date} to a Timestamp in UTC representing the same
     * point in time.
     * <p>
     * The resulting Timestamp will be precise to the millisecond.
     *
     * @return
     *          a new Timestamp instance, in UTC, precise to the millisecond;
     *          {@code null} if {@code date} is {@code null}
     *
     * @since IonJava R13
     */
    public static Timestamp forDateZ(Date date)
    {
        if (date == null) return null;
        long millis = date.getTime();
        return new Timestamp(millis, UTC_OFFSET);
    }


    /**
     * Converts a {@link java.sql.Timestamp} to a Timestamp in UTC representing
     * the same point in time.
     * <p>
     * The resulting Timestamp will be precise to the nanosecond.
     *
     * @param sqlTimestamp assumed to have nanoseconds precision
     *
     * @return
     *          a new Timestamp instance, in UTC, precise to the
     *          nanosecond
     *          {@code null} if {@code sqlTimestamp} is {@code null}
     *
     * @since IonJava R13
     */
    public static Timestamp forSqlTimestampZ(java.sql.Timestamp sqlTimestamp)
    {
        if (sqlTimestamp == null) return null;

        long millis = sqlTimestamp.getTime();
        Timestamp ts = new Timestamp(millis, UTC_OFFSET);
        int nanos = sqlTimestamp.getNanos();
        BigDecimal frac = BigDecimal.valueOf(nanos).movePointLeft(9);
        ts._fraction = frac;
        return ts;
    }


    /**
     * Returns a Timestamp representing the current time in milliseconds from
     * the epoch using the JVM clock, with an unknown local offset.
     * <p>
     * The resulting Timestamp will be precise to the millisecond.
     *
     * @return
     *          a new Timestamp instance representing the current time in
     *          milliseconds from the epoch (1970-01-01T00:00:00.000Z)
     */
    public static Timestamp now()
    {
        long millis = System.currentTimeMillis();
        return new Timestamp(millis, UNKNOWN_OFFSET);
    }

    /**
     * Returns a Timestamp in UTC representing the current time in milliseconds
     * from the epoch using the JVM clock.
     * <p>
     * The resulting Timestamp will be precise to the millisecond.
     *
     * @return
     *          a new Timestamp instance, in UTC, representing the current
     *          time in milliseconds from the epoch (1970-01-01T00:00:00.000Z)
     *
     * @since IonJava R13
     */
    public static Timestamp nowZ()
    {
        long millis = System.currentTimeMillis();
        return new Timestamp(millis, UTC_OFFSET);
    }


    /**
     * Converts the value of this Timestamp into a {@link Date},
     * representing the time in UTC.
     * <p>
     * This method will return the same result for all Timestamps representing
     * the same point in time, regardless of the local offset.
     * <p>
     * Because {@link Date} instances are mutable, this method returns a
     * new instance from each call.
     *
     * @return a new {@code Date} instance, in UTC
     */
    public Date dateValue()
    {
        long millis = getMillis();
        return new Date(millis);
    }


    /**
     * Converts the value of this Timestamp as a {@link Calendar}, in its
     * local time.
     * <p>
     * Because {@link Calendar} instances are mutable, this method returns a
     * new instance from each call.
     *
     * @return a new {@code Calendar} instance, in its local time.
     *
     * @since IonJava R13
     */
    public Calendar calendarValue()
    {
        Calendar cal = new GregorianCalendar(_Private_Utils.UTC);

        long millis = getMillis();
        Integer offset = _offset;
        if (offset != null && offset != 0)
        {
            int offsetMillis = offset * 60 * 1000;
            millis += offsetMillis;
            cal.setTimeInMillis(millis);                // Resets the offset!
            cal.set(Calendar.ZONE_OFFSET, offsetMillis);
        }
        else
        {
            cal.setTimeInMillis(millis);
        }

        return cal;
    }


    /**
     * Returns a number representing the Timestamp's point in time that is
     * the number of milliseconds (<em>ignoring</em> any fractional milliseconds)
     * from the epoch.
     * <p>
     * This method will return the same result for all Timestamps representing
     * the same point in time, regardless of the local offset.
     *
     * @return
     *          number of milliseconds (<em>ignoring</em> any fractional
     *          milliseconds) from the epoch (1970-01-01T00:00:00.000Z)
     */
    @SuppressWarnings("deprecation")
    public long getMillis()
    {
        //                                        month is 0 based for Date
        long millis = Date.UTC(this._year - 1900, this._month - 1, this._day, this._hour, this._minute, this._second);
        if (this._precision == Precision.FRACTION) {
            int frac = this._fraction.movePointRight(3).intValue();
            millis += frac;
        }
        return millis;

    }

    /**
     * Returns a BigDecimal representing the Timestamp's point in time that is
     * the number of milliseconds (<em>including</em> any fractional milliseconds)
     * from the epoch.
     * <p>
     * This method will return the same result for all Timestamps representing
     * the same point in time, regardless of the local offset.
     *
     * @return
     *          number of milliseconds (<em>including</em> any fractional
     *          milliseconds) from the epoch (1970-01-01T00:00:00.000Z)
     */
    @SuppressWarnings("deprecation")
    public BigDecimal getDecimalMillis()
    {
        long       millis;
        BigDecimal dec;

        switch (this._precision) {
        case YEAR:
        case MONTH:
        case DAY:
        case MINUTE:
        case SECOND:
            millis = Date.UTC(this._year - 1900, this._month - 1, this._day, this._hour, this._minute, this._second);
            return BigDecimal.valueOf(millis);
        case FRACTION:
            millis = Date.UTC(this._year - 1900, this._month - 1, this._day, this._hour, this._minute, this._second);
            dec = BigDecimal.valueOf(millis);
            dec = dec.add(this._fraction.movePointRight(3));
            return dec;
        }
        throw new IllegalArgumentException();
    }


    /**
     * Returns the precision of this Timestamp.
     */
    public Precision getPrecision()
    {
        return this._precision;
    }

    /**
     * Returns the offset of this Timestamp, measured in minutes, for the local
     * timezone in UTC.
     * <p>
     * For example, calling this method on Timestamps of:
     * <ul>
     *     <li>{@code 1969-02-23T07:00+07:00} will return {@code 420}</li>
     *     <li>{@code 1969-02-22T22:45:00.00-01:15} will return {@code -75}</li>
     *     <li>{@code 1969-02-23} (by Ion's definition, equivalent to
     *     {@code 1969-02-23T00:00-00:00}) will return {@code null}</li>
     * </ul>
     *
     * @return
     *          {@code null} if the local offset is unknown
     *          (i.e. {@code -00:00})
     */
    public Integer getLocalOffset()
    {
        return _offset;
    }


    /**
     * Returns the year of this Timestamp, in its local time.
     *
     * @return
     *          a number within the range [1, 9999], in its local time
     */
    public int getYear()
    {
        Timestamp adjusted = this;

        if (this._offset != null) {
            if (this._offset.intValue() != 0) {
                adjusted = make_localtime();
            }
        }
        return adjusted._year;
    }


    /**
     * Returns the month of this Timestamp, in its local time.
     *
     * @return
     *          a number within the range [1, 12], whereby 1 refers to January
     *          and 12 refers to December, in its local time;
     *          1 is returned if the Timestamp isn't precise to
     *          the month
     */
    public int getMonth()
    {
        Timestamp adjusted = this;

        if (this._offset != null) {
            if (this._offset.intValue() != 0) {
                adjusted = make_localtime();
            }
        }
        return adjusted._month;
    }


    /**
     * Returns the day (within the month) of this Timestamp, in its local time.
     *
     * @return
     *          a number within the range [1, 31], in its local time;
     *          1 is returned if the Timestamp isn't
     *          precise to the day
     */
    public int getDay()
    {
        Timestamp adjusted = this;
        if (this._offset != null) {
            if (this._offset.intValue() != 0) {
                adjusted = make_localtime();
            }
        }
        return adjusted._day;
    }


    /**
     * Returns the hour of this Timestamp, in its local time.
     *
     * @return
     *          a number within the range [0, 23], in its local time;
     *          0 is returned if the Timestamp isn't
     *          precise to the hour
     */
    public int getHour()
    {
        Timestamp adjusted = this;
        if (this._offset != null) {
            if (this._offset.intValue() != 0) {
                adjusted = make_localtime();
            }
        }
        return adjusted._hour;
    }


    /**
     * Returns the minute of this Timestamp, in its local time.
     *
     * @return
     *          a number within the range [0, 59], in its local time;
     *          0 is returned if the Timestamp isn't
     *          precise to the minute
     */
    public int getMinute()
    {
        Timestamp adjusted = this;
        if (this._offset != null) {
            if (this._offset.intValue() != 0) {
                adjusted = make_localtime();
            }
        }
        return adjusted._minute;
    }


    /**
     * Returns the second of this Timestamp.
     * <p>
     * Seconds are not affected by local offsets.
     * As such, this method produces the same output as {@link #getZSecond()}.
     *
     * @return
     *          a number within the range [0, 59];
     *          0 is returned if the Timestamp isn't precise to the second
     *
     * @see #getZSecond()
     */
    public int getSecond()
    {
        return this._second;
    }


    /**
     * Returns the fractional second of this Timestamp.
     * <p>
     * Fractional seconds are not affected by local offsets.
     * As such, this method produces the same output as
     * {@link #getZFractionalSecond()}.
     *
     * @return
     *          a BigDecimal within the range [0, 1);
     *          {@code null} is returned if the Timestamp isn't
     *          precise to the fractional second
     *
     * @see #getZFractionalSecond()
     */
    public BigDecimal getFractionalSecond()
    {
        return this._fraction;
    }


    /**
     * Returns the year of this Timestamp, in UTC.
     *
     * @return
     *          a number within the range [1, 9999], in UTC
     */
    public int getZYear()
    {
        return this._year;
    }


    /**
     * Returns the month of this Timestamp, in UTC.
     *
     * @return
     *          a number within the range [1, 12], whereby 1 refers to January
     *          and 12 refers to December, in UTC;
     *          1 is returned if the Timestamp isn't precise to
     *          the month
     */
    public int getZMonth()
    {
        return this._month;
    }


    /**
     * Returns the day of this Timestamp, in UTC.
     *
     * @return
     *          a number within the range [1, 31], in UTC;
     *          1 is returned if the Timestamp isn't
     *          precise to the day
     */
    public int getZDay()
    {
        return this._day;
    }


    /**
     * Returns the hour of this Timestamp, in UTC.
     *
     * @return
     *          a number within the range [0, 23], in UTC;
     *          0 is returned if the Timestamp isn't
     *          precise to the hour
     */
    public int getZHour()
    {
        return this._hour;
    }


    /**
     * Returns the minute of this Timestamp, in UTC.
     *
     * @return
     *          a number within the range [0, 59], in UTC;
     *          0 is returned if the Timestamp isn't
     *          precise to the minute
     */
    public int getZMinute()
    {
        return this._minute;
    }


    /**
     * Returns the second of this Timestamp.
     * <p>
     * Seconds are not affected by local offsets.
     * As such, this method produces the same output as {@link #getSecond()}.
     *
     * @return
     *          a number within the range [0, 59];
     *          0 is returned if the Timestamp isn't precise to the second
     *
     * @see #getSecond()
     */
    public int getZSecond()
    {
        return this._second;
    }


    /**
     * Returns the fractional second of this Timestamp.
     * <p>
     * Fractional seconds are not affected by local offsets.
     * As such, this method produces the same output as
     * {@link #getFractionalSecond()}.
     *
     * @return
     *          a BigDecimal within the range [0, 1);
     *          {@code null} is returned if the Timestamp isn't
     *          precise to the fractional second
     *
     * @see #getFractionalSecond()
     */
    public BigDecimal getZFractionalSecond()
    {
        return this._fraction;
    }


    /**
     * Returns the string representation (in Ion format) of this Timestamp in
     * its local time.
     *
     * @see #toZString()
     * @see #print(Appendable)
     */
    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder(32);
        try
        {
            print(buffer);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Exception printing to StringBuilder",
                                       e);
        }
        return buffer.toString();
    }


    /**
     * Returns the string representation (in Ion format) of this Timestamp
     * in UTC.
     *
     * @see #toString()
     * @see #printZ(Appendable)
     */
    public String toZString()
    {
        StringBuilder buffer = new StringBuilder(32);
        try
        {
            printZ(buffer);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Exception printing to StringBuilder",
                                       e);
        }
        return buffer.toString();
    }


    /**
     * Prints to an {@code Appendable} the string representation (in Ion format)
     * of this Timestamp in its local time.
     * <p>
     * This method produces the same output as {@link #toString()}.
     *
     * @param out not {@code null}
     *
     * @throws IOException propagated when the {@link Appendable} throws it
     *
     * @see #printZ(Appendable)
     */
    public void print(Appendable out)
        throws IOException
    {
        // we have to make a copy to preserve the "immutable" contract
        // on Timestamp and we don't want someone reading the calendar
        // member while we've shifted it around.
        Timestamp adjusted = this;

        // Adjust UTC time back to local time
        if (this._offset != null && this._offset.intValue() != 0) {
            adjusted = make_localtime();
        }

        print(out, adjusted);
    }


    /**
     * Prints to an {@code Appendable} the string representation (in Ion format)
     * of this Timestamp in UTC.
     * <p>
     * This method produces the same output as {@link #toZString()}.
     *
     * @param out not {@code null}
     *
     * @throws IOException propagated when the {@code Appendable} throws it.
     *
     * @see #print(Appendable)
     */
    public void printZ(Appendable out)
        throws IOException
    {
        Timestamp ztime = this.clone();
        ztime._offset = UTC_OFFSET;
        ztime.print(out);
    }


    /**
     * helper for print(out) and printZ(out) so that printZ can create
     * a zulu time and pass it directly and print can apply the local
     * offset and adjust the various fields (without breaking the
     * contract to be immutable).
     * @param out destination for the text image of the value
     * @param adjusted the time value with the fields adjusted to match the desired text output
     * @throws IOException
     */
    private static void print(Appendable out, Timestamp adjusted)
        throws IOException
    {
        // null is our first "guess" to get it out of the way
        if (adjusted == null) {
            out.append("null.timestamp");
            return;
        }

        // so we have a real value - we'll start with the date portion
        // which we always have
        print_digits(out, adjusted._year, 4);
        if (adjusted._precision == Precision.YEAR) {
            out.append("T");
            return;
        }

        out.append("-");
        print_digits(out, adjusted._month, 2);  // convert calendar months to a base 1 value
        if (adjusted._precision == Precision.MONTH) {
            out.append("T");
            return;
        }

        out.append("-");
        print_digits(out, adjusted._day, 2);
        if (adjusted._precision == Precision.DAY && adjusted._offset == null) {
            // out.append("T");
            return;
        }

        // see if we have some time
        if (adjusted._precision == Precision.MINUTE
            || adjusted._precision == Precision.SECOND
            || adjusted._precision == Precision.FRACTION
        ) {
            out.append("T");
            print_digits(out, adjusted._hour, 2);
            out.append(":");
            print_digits(out, adjusted._minute, 2);
            // ok, so how much time do we have ?
            if (adjusted._precision == Precision.SECOND
                || adjusted._precision == Precision.FRACTION
            ) {
                out.append(":");
                print_digits(out, adjusted._second, 2);
            }
            if (adjusted._precision == Precision.FRACTION) {
                print_fractional_digits(out, adjusted._fraction);
            }
        }
        if (adjusted._offset != null) {
            int min, hour;
            min = adjusted._offset;
            if (min == 0) {
                out.append('Z');
            }
            else {
                if (min < 0) {
                    min = -min;
                    out.append('-');
                }
                else {
                    out.append('+');
                }
                hour = min / 60;
                min = min - hour*60;
                print_digits(out, hour, 2);
                out.append(":");
                print_digits(out, min, 2);
            }
        }
        else {
            out.append("-00:00");
        }
    }
    private static void print_digits(Appendable out, int value, int length)
        throws IOException
    {
        char temp[] = new char[length];
        while (length > 0) {
            length--;
            int next = value / 10;
            temp[length] =  (char)('0' + (value - next*10));
            value = next;
        }
        while (length > 0) {
            length--;
            temp[length] =  '0';
        }
        for (char c : temp) {
            out.append(c);
        }
    }
    private static void print_fractional_digits(Appendable out, BigDecimal value)
        throws IOException
    {
        String temp = value.toPlainString(); // crude, but it works
        if (temp.charAt(0) == '0') { // this should always be true
            temp = temp.substring(1);
        }
        out.append(temp);
    }

    @Override
    public int hashCode()
    {
        // Performs a Shift-Add-XOR-Rotate hash. Rotating at each step to
        // produce an "Avalanche" effect for timestamps with small deltas, which
        // is found to be a common input data set.
        // Fixes ION-310.

        final int prime = 8191;
        int result = HASH_SIGNATURE;

        result = prime * result + (this._precision == Precision.FRACTION
            ? _fraction.hashCode()
            : 0);

        result ^= (result << 19) ^ (result >> 13);

        result = prime * result + this._year;
        result = prime * result + this._month;
        result = prime * result + this._day;
        result = prime * result + this._hour;
        result = prime * result + this._minute;
        result = prime * result + this._second;

        result ^= (result << 19) ^ (result >> 13);

        result = prime * result + this._precision.toString().hashCode();

        result ^= (result << 19) ^ (result >> 13);

        result = prime * result + (_offset == null ? 0 : _offset.hashCode());

        result ^= (result << 19) ^ (result >> 13);

        return result;
    }



    /**
     * Performs a comparison of the two points in time represented by two
     * Timestamps.
     * If the point in time represented by this Timestamp precedes that of
     * {@code t}, then {@code -1} is returned.
     * If {@code t} precedes this Timestamp then {@code 1} is returned.
     * If the Timestamps represent the same point in time, then
     * {@code 0} is returned.
     * Note that a {@code 0} result does not imply that the two Timestamps are
     * {@link #equals}, as the local offset or precision of the two Timestamps
     * may be different.
     *
     * <p>
     * This method is provided in preference to individual methods for each of
     * the six boolean comparison operators (<, ==, >, >=, !=, <=).
     * The suggested idiom for performing these comparisons is:
     * {@code (x.compareTo(y)}<em>&lt;op></em>{@code 0)},
     * where <em>&lt;op></em> is one of the six comparison operators.
     *
     * <p>
     * For example, the pairs below will return a {@code 0} result:
     * <ul>
     *   <li>{@code 2009T}</li>
     *   <li>{@code 2009-01T}</li>
     *   <li>{@code 2009-01-01T}</li>
     *   <li>{@code 2009-01-01T00:00Z}</li>
     *   <li>{@code 2009-01-01T00:00:00Z}</li>
     *   <li>{@code 2009-01-01T00:00:00.0Z}</li>
     *   <li>{@code 2009-01-01T00:00:00.00Z}</li>
     *
     *   <li>{@code 2008-12-31T16:00-08:00}</li>
     *   <li>{@code 2008-12-31T12:00-12:00}</li>
     *   <li>{@code 2009-01-01T12:00+12:00}</li>
     * </ul>
     *
     * <p>
     * Use the {@link #equals(Timestamp)} method to compare the point
     * in time, <em>including</em> precision and local offset.
     *
     * @param t
     *          the other {@code Timestamp} to compare this {@code Timestamp} to
     *
     * @return
     *          -1, 0, or 1 if this {@code Timestamp}
     *          is less than, equal to, or greater than {@code t} respectively
     *
     * @throws NullPointerException if {@code t} is null.
     *
     * @see #equals(Timestamp)
     */
    public int compareTo(Timestamp t)
    {
        // Test at millisecond precision first.
        long this_millis = this.getMillis();
        long arg_millis = t.getMillis();
        if (this_millis != arg_millis) {
            return (this_millis < arg_millis) ? -1 : 1;
        }

        // Values are equivalent at millisecond precision, so compare fraction

        BigDecimal this_fraction =
            ((this._fraction == null) ? BigDecimal.ZERO : this._fraction);
        BigDecimal arg_fraction =
            (( t._fraction == null) ? BigDecimal.ZERO :  t._fraction);
        return this_fraction.compareTo(arg_fraction);
    }


    /**
     * Compares this {@link Timestamp} to the specified Object.
     * The result is {@code true} if and only if the parameter is a
     * {@link Timestamp} object that represents the same point in time,
     * precision and local offset as this Timestamp.
     * <p>
     * Use the {@link #compareTo(Timestamp)} method to compare only the point
     * in time, <em>ignoring</em> precision and local offset.
     *
     * @see #equals(Timestamp)
     * @see #compareTo(Timestamp)
     */
    @Override
    public boolean equals(Object t)
    {
        if (!(t instanceof Timestamp)) return false;
        return equals((Timestamp)t);
    }

    /**
     * Compares this {@link Timestamp} to another {@link Timestamp} object.
     * The result is {@code true} if and only if the parameter
     * represents the same point in time and has
     * the same precision and local offset as this object.
     * <p>
     * These pairs are {@link #equals} to each other, as they
     * represent the same points in time, precision and local offset:
     *
     * <ul>
     *   <li>{@code 2001-01-01T11:22+00:00} (minute precision, in UTC)</li>
     *   <li>{@code 2001-01-01T11:22Z} (minute precision, in UTC)</li>
     * </ul>
     *
     * <p>
     * On the other hand, none of these pairs are {@link #equals} to each other,
     * they represent the same points in time, but with different precisions
     * and/or local offsets:
     *
     * <ul>
     *   <li>{@code 2001T} (year precision, unknown local offset)</li>
     *   <li>{@code 2001-01T} (month precision, unknown local offset)</li>
     *   <li>{@code 2001-01-01T} (day precision, unknown local offset)</li>
     *
     *   <li>{@code 2001-01-01T00:00-00:00} (second precision, unknown local offset)</li>
     *   <li>{@code 2001-01-01T00:00+00:00} (second precision, in UTC)</li>
     *
     *   <li>{@code 2001-01-01T00:00.000-00:00} (millisecond precision, unknown local offset)</li>
     *   <li>{@code 2001-01-01T00:00.000+00:00} (millisecond precision, in UTC)</li>
     * </ul>
     *
     * <p>
     * Use the {@link #compareTo(Timestamp)} method to compare only the point
     * in time, <em>ignoring</em> precision and local offset.
     *
     * @see #compareTo(Timestamp)
     */
    public boolean equals(Timestamp t)
    {
        if (this == t) return true;
        if (t == null) return false;

        // if the precisions are not the same the values are not
        // precision doesn't matter WRT equality
        if (this._precision != t._precision) return false;

        // if the local offset are not the same the values are not
        if (this._offset == null) {
            if (t._offset != null)  return false;
        }
        else {
            if (t._offset == null) return false;
        }

        // so now we check the actual time value
        if (this._year   != t._year)    return false;
        if (this._month  != t._month)   return false;
        if (this._day    != t._day)     return false;
        if (this._hour   != t._hour)    return false;
        if (this._minute != t._minute)  return false;
        if (this._second != t._second)  return false;

        // and if we have a local offset, check the value here
        if (this._offset != null) {
            if (this._offset.intValue() != t._offset.intValue()) return false;
        }

        // we only look at the fraction if we know that it's actually there
        if (this._precision == Precision.FRACTION) {
            if (this._fraction == null) {
                if (t._fraction != null) return false;
            }
            else {
                if (t._fraction == null) return false;
            }
            if (!this._fraction.equals(t._fraction)) return false;
        }
        return true;
    }
}
