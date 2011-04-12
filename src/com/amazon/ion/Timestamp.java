// Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.util.IonTextUtils.printCodePointAsString;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;

/**
 * An immutable representation of time.  Ion defines a simple representational
 * form of date and time but this class includes support for accepting the commonly
 * used Java "milliseconds since the UNIX epoch" form of working with time.  The
 * expectation is that the value is (essentially) UTC time.  In reality the practical
 * use of time could be more accurately described of UTC-SLS (UTC Smoothed Leap Seconds)
 * as there is no representation for the 24 (as of February 2009) leap second
 * discontinuities that UTC has added.
 * <p>
 * This implementation preserves the "signficant digits" of the value.  The
 * precision defines which fields have been included.  Only common break
 * points in the values are supported.  Any unspecified values are treated
 * as 0 (aka midnight, beginning of the new day).
 *
 * <h4>Equality and Comparison</h4>
 *
 * As with {@link IonValue} classes, the {@link #equals} methods on this class
 * performs a strict equivalence that observes the precision of each timestamp.
 * This means that it's possible to have two {@link Timestamp} instances that
 * represent the same point in time but are not equivalent.
 * <p>
 * On the other hand, the {@link #compareTo} methods perform timeline
 * comparison, ignoring precision. Thus the <em>natural comparison method</em>
 * of this class is <em>not consistent with equals</em>. See the documentation
 * of {@link Comparable} for further discussion.
 * <p>
 * To illustrate this distinction, consider the following timestamps. None are
 * {@link #equals} to each other, but any pair will return a zero result from
 * {@link #compareTo}.
 * <ul>
 *   <li>{@code 2009T}</li>
 *   <li>{@code 2009-01T}</li>
 *   <li>{@code 2009-01-01T}</li>
 *   <li>{@code 2009-01-01T00:00Z}</li>
 *   <li>{@code 2009-01-01T00:00.0Z}</li>
 *   <li>{@code 2009-01-01T00:00.00Z}</li>
 *   <li>{@code 2009-01-01T00:00.000Z} <em>etc.</em></li>
 * </ul>
 */

public final class Timestamp
    implements Comparable, Cloneable
{
    public final static Integer UNKNOWN_OFFSET = null;
    public final static Integer UTC_OFFSET = new Integer(0);

    public static enum Precision {
        YEAR,
        MONTH,
        DAY,
        MINUTE,
        SECOND,
        FRACTION
    }

//    private static final int BIT_FLAG_YEAR      = 0x01;
//    private static final int BIT_FLAG_MONTH     = 0x02;
//    private static final int BIT_FLAG_DAY       = 0x04;
//    private static final int BIT_FLAG_MINUTE    = 0x08;
//    private static final int BIT_FLAG_SECOND    = 0x10;
//    private static final int BIT_FLAG_FRACTION  = 0x20;
//
//
//    static public int getPrecisionAsBitFlags(Precision p) {
//        int precision_flags = 0;
//
//        // fall through each case - by design - to accumulate all necessary bits
//        switch (p) {
//        default:        throw new IllegalStateException("unrecognized precision"+p);
//        case FRACTION:  precision_flags |= BIT_FLAG_FRACTION;
//        case SECOND:    precision_flags |= BIT_FLAG_SECOND;
//        case MINUTE:    precision_flags |= BIT_FLAG_MINUTE;
//        case DAY:       precision_flags |= BIT_FLAG_DAY;
//        case MONTH:     precision_flags |= BIT_FLAG_MONTH;
//        case YEAR:      precision_flags |= BIT_FLAG_YEAR;
//        }
//
//        return precision_flags;
//    }
//
//    static public boolean precisionIncludes(int precision_flags, Precision isIncluded)
//    {
//        switch (isIncluded) {
//        case FRACTION:  return (precision_flags & BIT_FLAG_FRACTION) != 0;
//        case SECOND:    return (precision_flags & BIT_FLAG_SECOND) != 0;
//        case MINUTE:    return (precision_flags & BIT_FLAG_MINUTE) != 0;
//        case DAY:       return (precision_flags & BIT_FLAG_DAY) != 0;
//        case MONTH:     return (precision_flags & BIT_FLAG_MONTH) != 0;
//        case YEAR:      return (precision_flags & BIT_FLAG_YEAR) != 0;
//        default:        break;
//        }
//        throw new IllegalStateException("unrecognized precision"+isIncluded);
//    }

    /**
     * The precision of this value.  This encodes the precision that the
     * original value was defined to have.  All Timestamps have a
     * date value but with reduced precision they may exclude any
     * time values.  Seconds may be excluded.  And the exact precision
     * of the seconds is controlled by the BigDecimal fraction.
     * This member also encodes whether or not the value is null.
     */
    private Precision     _precision;

    /**
     * These keep the basic struct values for the timestamp.
     * _month and _day are 1 based (0 is an invalid value for
     * these in a non-null Timestamp.
     */
    private short   _year;
    private byte    _month = 1; // Initialized to valid default
    private byte    _day   = 1; // Initialized to valid default
    private byte    _hour;
    private byte    _minute;
    private byte    _second;
    private BigDecimal     _fraction;  // fractional seconds, this will be between >= 0 and < 1

    /**
     * Minutes offset from UTC; zero means UTC proper,
     * <code>null</code> means that the offset is unknown.
     */
    private Integer     _offset;

                                                //jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec
                                                // the first 0 is to make these arrays 1 based (since month values are 1-12)
    private static int[] _Leap_days_in_month   = { 0,  31,  29,  31,  30,  31,  30,  31,  31,  30,  31,  30,  31 };
    private static int[] _Normal_days_in_month = { 0,  31,  28,  31,  30,  31,  30,  31,  31,  30,  31,  30,  31 };

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
        return is_leap ? _Leap_days_in_month[month] : _Normal_days_in_month[month];
    }

    /**
     * this changes the year-minute values with rollover to apply
     * the timezone offset to the local struct values.
     *
     * @param offset the local offset, in minutes from GMT.
     */
    private void apply_offset(int offset)
    {
        if (offset == 0) return;
        if (offset < -24*60 || offset > 24*60) {
            throw new IllegalArgumentException("bad offset " + offset);
        }
        // To convert _to_ GMT you must SUBTRACT the local offset
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
        // so this adjusts the value back to zulu time (GMT)
        // Note that the sign on this is opposite of Ion (and Calendar) offset.
        // Example: PST = 480 here but Ion/Calendar use -480 = -08:00 = GMT-8
        int offset = date.getTimezoneOffset();
        this.apply_offset(-offset);
    }
    private void set_fraction_from_millis(long ms) {
        this._precision = Precision.FRACTION;
        // BigDecimal is immutable - so really we're resetting this value
        long secs = ms / 1000L;
        int msInt = (int)(ms - secs*1000L);
        BigDecimal dec_ms = new BigDecimal(msInt);
        dec_ms = dec_ms.movePointLeft(3); // set value to milliseconds
        this._fraction = dec_ms;
    }
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
            throw new IllegalArgumentException("unset Calendar is not valid for set");
        }

        copy_from_calendar_fields(this._precision, cal);
        // TODO why doesn't this happen in copy_from_calendar_fields ?
        if (this._precision == Precision.FRACTION) {
            BigDecimal millis = new BigDecimal(cal.get(Calendar.MILLISECOND));
            this._fraction = millis.movePointRight(3); // make these actually 1/1000's of a second
        }
    }

    /**
     * this creates a new calendar member on this value.  The new member
     * will have some of it's fields set, based on the precision specified.
     * This does not do anything with the millisecond field.  The calendar
     * millisecond field is not used internally, but may be specified if
     * an external party passes in the calendar instance.  The millisecond
     * portion needs to be handled by the caller if necessary.
     * @param p the precision which control which fields are copied
     * @param cal the source calendar value
     */
    private void copy_from_calendar_fields(Precision p, Calendar cal) {
        assert this._day == 1;
        switch (p) {
            case FRACTION:
            case SECOND:
                this._second = (byte)cal.get(Calendar.SECOND);
            case MINUTE:
                this._hour   = (byte)cal.get(Calendar.HOUR_OF_DAY);
                this._minute = (byte)cal.get(Calendar.MINUTE);
            case DAY:
                this._day    = (byte)cal.get(Calendar.DAY_OF_MONTH);
            case MONTH:
                // calendar months are 0 based, timestamp months are 1 based
                this._month  = (byte)(cal.get(Calendar.MONTH) + 1);
            case YEAR:
                this._year   = (short)cal.get(Calendar.YEAR);
        }

        // Strangely, if this test is made before calling get(), it will return
        // false even when Calendar.setTimeZone() was called.
        if (cal.isSet(Calendar.ZONE_OFFSET)) {
            // convert ms to minutes
            _offset = cal.get(Calendar.ZONE_OFFSET) / (1000*60);
            // Transform our members from local time to Zulu
            this.apply_offset(_offset);
        }
    }


    private void validate_fields()
    {
        if (_year < 1  || _year > 9999) error_in_field("year must be between 1 and 9999 inclusive GMT, and local time");
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
        //throw new IonException(e);
        throw e;
    }

    /**
     * Creates a new Timestamp, precise to the date,
     * with unknown local offset.
     * <p>
     * This is equivalent to the corresponding Ion value {@code YYYY-MM-DD}.
     */
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
     * Creates a new Timestamp,precise to the minute,
     * with a given local offset.
     * <p>
     * This is equivalent to the corresponding Ion value
     * {@code YYYY-MM-DDThh:mm+-oo:oo}.
     *
     * @param offset may be null to indicate unknown local offset.
     */
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
     * Creates a new Timestamp, precise to the second,
     * with a given local offset.
     * <p>
     * This is equivalent to the corresponding Ion value
     * {@code YYYY-MM-DDThh:mm:ss+-oo:oo}.
     *
     * @param offset may be null to indicate unknown local offset.
     */
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
     * Creates a new Timestamp, precise to the fractional second,
     * with a given local offset.
     * <p>
     * This is equivalent to the corresponding Ion value
     * {@code YYYY-MM-DDThh:mm:ss.fff+-oo:oo}.
     *
     * @param frac must not be null.  If negative, the absolute value is used.
     * @param offset may be null to indicate unknown local offset.
     *
     * @throws NullPointerException if {@code frac} is {@code null}.
     */
    public Timestamp(int year, int month, int day,
                     int hour, int minute, int second, BigDecimal frac,
                     Integer offset)
    {
        this._precision = Precision.FRACTION;
        this._fraction = frac.abs();
        this._second   = (byte)second;
        this._minute   = (byte)minute;
        this._hour     = (byte)hour;
        this._day      = (byte)day;  // days are base 1 (as you'd expect)
        this._month    = (byte)month;
        this._year     = (short)year;

        validate_fields();
        if (offset != null) {
        this._offset = offset;
        apply_offset(offset);
    }
    }

    /**
     * Creates a new Timestamp from the individual struct components.
     * This is primarily used by the IonBinary reader, which reads the
     * individual pieces out of the byte stream to build up the timestamp.
     * The date/time values are expected to be the zulu time, the offset
     * is applied (so other local date and time fields may change as the
     * field members reflect GMT time, not local time.
     * This is the "native" constructor which sets all the values
     * appropriately and directly.
     */
    private Timestamp(Precision p, int zyear, int zmonth, int zday,
                      int zhour, int zminute, int zsecond, BigDecimal frac,
                      Integer offset)
    {
        this._precision = p;
        switch (p) {
        default:
            throw new IllegalArgumentException("invalid Precision passed to constructor");
        case FRACTION:
            this._fraction = frac.abs();
        case SECOND:
            this._second = (byte)zsecond;
        case MINUTE:
            this._minute = (byte)zminute;
            this._hour   = (byte)zhour;
        case DAY:
            this._day    = (byte)zday;  // days are base 1 (as you'd expect)
        case MONTH:
            this._month  = (byte)zmonth;
        case YEAR:
            this._year   = (short)zyear;
        }
        validate_fields();
        this._offset = offset;
        // This doesn't call applyOffset() like the other constructors because
        // we already expect Zulu field valus, and that's what we're supposed
        // to have.
    }


    /**
     * Creates a new Timestamp from the individual struct components.
     * This is primarily used by the IonBinary reader, which reads the
     * individual pieces out of the byte stream to build up the timestamp.
     * The date/time values are expected to be the zulu time, the offset
     * is applied (so other local date and time fields may change as the
     * field members reflect GMT time, not local time.
     * This is the "native" constructor which sets all the values
     * appropriately and directly.
     *
     * @param offset may be null to indicate unknown local offset.
     */
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
     * Creates a new Timestamp instance from an instance of Calendar.  This
     * uses the calendar's offset as the offset.
     *
     * @param cal Calendar time value to base new instance time on
     */
    public Timestamp(Calendar cal)
    {
        set_fields_from_calendar(cal);
    }

    /**
     * Creates a Timestamp at the specified UTC point in time.
     * @param millis must not be {@code null}.
     * @param localOffset may be {@code null} to represent unknown local
     * offset.
     */
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
            long secs = ms / 1000;
            BigDecimal temp = millis.movePointLeft(3);
            BigDecimal dsec = new BigDecimal(secs);
            this._fraction = temp.subtract(dsec);
        }
        this._offset = localOffset;
        this.validate_fields();
    }

    /**
     * Creates a new Timestamp instance whose value is the UTC time
     * specified by the passed in UTC milliseconds from epoch value after
     * adjust of any localOffset that may be included.  The new value's
     * precision will be set to be milliseconds.
     *
     * @param localOffset may be {@code null} to represent unknown local
     * offset.
     */
    public Timestamp(long millis, Integer localOffset)
    {
        Date d = new Date(millis);  // this will have the system timezone in it whether we want it or not
        this.set_fields_from_millis(d);
        this.set_fraction_from_millis(millis);
        this._offset = localOffset;
        this.validate_fields();
    }

    /**
     * parse the characters passed in to create a new instance.  This will throw an IllegalArgumentException
     * in the event the format of the value is not a valid Ion Timestamp.  This will ignore extra characters
     * if the sequence contains more than just the 1 value.  It will check the character immediately
     * after the date-time value and will throw if the character is not a valid Ion Timestamp terminating character.
     * @param image CharSequence
     * @throws IllegalArgumentException if the string is not a valid timestamp image
     */
    public static Timestamp valueOf(CharSequence image) {
        final String NULL_TIMESTAMP_IMAGE = "null.timestamp";
        final int    LEN_OF_NULL_IMAGE    = NULL_TIMESTAMP_IMAGE.length();
        final int    END_OF_YEAR          =  4;  // 1234T
        final int    END_OF_MONTH         =  7;  // 1234-67T
        final int    END_OF_DAY           = 10;  // 1234-67-90T
        final int    END_OF_MINUTES       = 16;
        final int    END_OF_SECONDS       = 19;

        int temp, pos;
        int length = -1;

        if (image == null || image.length() < 1) {
            throw new IllegalArgumentException("empty timestamp");
        }
        length = image.length();

        // check for 'null.timestamp'
        if (image.charAt(0) == 'n') {
            if (length >= LEN_OF_NULL_IMAGE
                && NULL_TIMESTAMP_IMAGE.equals(image.subSequence(0, LEN_OF_NULL_IMAGE).toString()))
            {
                if (length > LEN_OF_NULL_IMAGE) {
                    if (!isValidFollowChar(image.charAt(LEN_OF_NULL_IMAGE))) {
                        throw new IllegalArgumentException("invalid timestamp: " + image);
                    }
                }
                return null;
            }
            throw new IllegalArgumentException("invalid timestamp: " + image);
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
            year  = read_digits(image, 0, 4, -1, "year");

            char c = image.charAt(END_OF_YEAR);
            if (c == 'T') break;
            if (c != '-') {
                throw new IllegalArgumentException("invalid timestamp: expected \"-\" between year and month, found " + printCodePointAsString(c));
            }
            if (length < END_OF_MONTH + 1) {  // +1 for the "T"
                throw new IllegalArgumentException("invalid timestamp image: year month form is too short (must be yyyy-mmT)");
            }
            pos = END_OF_MONTH;
            precision = Precision.MONTH;
            month = read_digits(image, END_OF_YEAR + 1, 2, -1, "month");

            c = image.charAt(END_OF_MONTH);
            if (c == 'T') break;
            if (c != '-') {
                throw new IllegalArgumentException("invalid timestamp: expected \"-\" between month and day, found " + printCodePointAsString(c));
            }
            if (length < END_OF_DAY) {
                throw new IllegalArgumentException("invalid timestamp image: too short for yyyy-mm-dd");
            }
            pos = END_OF_DAY;
            precision = Precision.DAY;
            day   = read_digits(image, END_OF_MONTH + 1, 2, -1, "day");
            if (length == END_OF_DAY) break;
            c = image.charAt(END_OF_DAY);
            if (c != 'T') {
                throw new IllegalArgumentException("invalid timestamp: expected \"T\" after day, found " + printCodePointAsString(c));
            }
            if (length == END_OF_DAY + 1) break;

            // now lets see if we have a time value
            if (length < END_OF_MINUTES) {
                throw new IllegalArgumentException("invalid timestamp image: too short for yyyy-mm-ddThh:mm");
            }
            hour   = read_digits(image, 11, 2, ':', "hour");
            minute = read_digits(image, 14, 2, -1, "minutes");
            pos = END_OF_MINUTES;
            precision = Precision.MINUTE;

            // we may have seconds
            if (length <= END_OF_MINUTES || image.charAt(END_OF_MINUTES) != ':') break;
            if (length < END_OF_SECONDS) {
                throw new IllegalArgumentException("invalid timestamp imagetoo short for yyyy-mm-ddThh:mm:ss");
            }
            seconds = read_digits(image, 17, 2, -1, "seconds");
            pos = END_OF_SECONDS;
            precision = Precision.SECOND;

            if (length <= END_OF_SECONDS || image.charAt(END_OF_SECONDS) != '.') break;
            precision = Precision.SECOND;
            pos = END_OF_SECONDS + 1;
            while (length > pos && Character.isDigit(image.charAt(pos))) {
                pos++;
            }
            if (pos <= END_OF_SECONDS + 1) {
                throw new IllegalArgumentException("Timestamp must have at least one digit after decimal point: " + image);
            }
            precision = Precision.FRACTION;
            fraction = new BigDecimal(image.subSequence(19, pos).toString());
        } while (false);

        Integer offset;

        // now see if they included a timezone offset
        char timezone_start = pos < length ? image.charAt(pos) : '\n';
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
            temp = read_digits(image, pos, 2, ':', "local offset hours");
            pos += 3;
            temp = temp * 60 + read_digits(image, pos, 2, -1, "local offset minutes");
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
                    error_in_field("Timestamp must have local offset: " + image);
            }
            offset = null;
        }
        if (image.length() > (pos + 1) && !isValidFollowChar(image.charAt(pos + 1))) {
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
        // we do pass the local offset to the constructor while our field values are
        // GMT irrespective of the offset the full-field constructor simply copies the
        // values across as it expects the GMT fields.
        Timestamp c = new Timestamp(this._precision, this._year, this._month, this._day, this._hour, this._minute, this._second, this._fraction, this._offset);
        return c;
    }

    /**
     * this is used by the get<FIELD> methods to adjust the current value
     * to be shifted into the local timezone
     * @return Timestamp with fields in local time
     */
    private Timestamp make_localtime()
    {
        int offset = this._offset != null ? this._offset.intValue() : 0;
        //Precision p = (this._precision == Precision.TT_FRAC) ? Precision.TT_SECS : this._precision;
        Timestamp localtime = new Timestamp(this._precision, this._year, this._month, this._day, this._hour, this._minute, this._second, this._fraction, null);
        localtime.apply_offset(-offset);
        localtime._offset = this._offset;  // FIXME Misleading
        //localtime._precision = this._precision;
        return localtime;
    }


    /**
     * this returns the current time using the JVM clock and an unknown timezone
     */
    static public Timestamp now()
    {
        long millis = System.currentTimeMillis();
        Timestamp t = new Timestamp(millis, null);
        return t;
    }


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
    @SuppressWarnings("deprecation")
    public Date dateValue()
    {
        //                                        month is 0 based for Date
        long millis = Date.UTC(this._year - 1900, this._month - 1, this._day, this._hour, this._minute, this._second);
        if (this._precision == Precision.FRACTION) {
            int frac = this._fraction.movePointRight(3).intValue();
            millis += frac;
        }
        Date d = new Date(millis);
        return d;
    }


    /**
     * Gets the value of this Ion <code>timestamp</code> as the number of
     * milliseconds since 1970-01-01T00:00:00.000Z, truncating any fractional
     * milliseconds.
     * This method will return the same result for all Ion representations of
     * the same instant, regardless of the local offset.
     *
     * @return the number of milliseconds since 1970-01-01T00:00:00.000Z
     * represented by this timestamp.
     */
    @SuppressWarnings("deprecation")
    public long getMillis()
    {
        long millis = Date.UTC(this._year - 1900, this._month - 1, this._day, this._hour, this._minute, this._second);
        if (this._precision == Precision.FRACTION) {
            int frac = this._fraction.movePointRight(3).intValue();
            millis += frac;
        }
        return millis;

    }

    /**
     * Gets the value of this Ion <code>timestamp</code> as the number of
     * milliseconds since 1970-01-01T00:00:00Z, including fractional
     * milliseconds.
     * This method will return the same result for all Ion representations of
     * the same instant, regardless of the local offset.
     *
     * @return the number of milliseconds since 1970-01-01T00:00:00Z
     * represented by this timestamp.
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
            return new BigDecimal(millis);
        case FRACTION:
            millis = Date.UTC(this._year - 1900, this._month - 1, this._day, this._hour, this._minute, this._second);
            dec = new BigDecimal(millis);
            dec = dec.add(this._fraction.movePointRight(3));
            return dec;
        }
        throw new IllegalArgumentException();
    }


    /**
     * get the Precision value from the Timestamp.
     * @return Precision timestamps precision
     */
    public Precision getPrecision()
    {
        return this._precision;
    }

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
     */
    public Integer getLocalOffset()
    {
        return _offset;
    }

    /**
     * get the year of this value in the local time.
     * @return int local time year
     */
    public int getYear()
    {
        Timestamp adjusted = this;

        if (this._offset != null) {
            int offset = this._offset.intValue();
            boolean needs_adjustment = false;
            if (offset < 0) {
                // look for the only case the year might roll over forward
                if (this._month == 12 && this._day == 31) {
                    needs_adjustment = true;
                }
            }
            else {
                // look for the only case the year might roll over forward
                if (this._month == 1 && this._day == 1) {
                    needs_adjustment = true;
                }
            }
            if (needs_adjustment) {
                adjusted = make_localtime();
            }
        }
        return adjusted._year;
    }

    /**
     * get the month of the year of this value in the local time.  January is month 1.
     * @return int local time month of the year
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
     * get the day of the month of this value in the local time.
     * @return int local time day of the month
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
     * get the hours field of the time of this value in the local time.
     * @return int local time hours
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
     * get the minutes of this value in the local time.
     * @return int local time minutes
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
     * get the seconds of this value in the local time.
     * @return int local time seconds
     */
    public int getSecond()
    {
        return this._second;
    }

    /**
     * get the fraction seconds portion of the time this value in the local time.
     * If the precision doesn't include fraction seconds this will return null.
     * @return int local time fractional seconds if specified otherwise null
     */
    public BigDecimal getFractionalSecond()
    {
        return this._fraction;
    }

    /**
     * get the year portion of the date of this value as a Zulu time (in GMT time).
     * @return int year portion of the date in Zulu time
     */
    public int getZYear()
    {
        return this._year;
    }

    /**
     * get the the month portion of the date of this value as a Zulu time (in GMT time).
     * In Timestamp January is month 1 (the GregorianCalendar class numbers months
     * from 0, but days of the month from 1).
     * @return int the month in Zulu time
     */
    public int getZMonth()
    {
        return this._month;
    }

    /**
     * get the day of the month portion of the date of this value as a Zulu time (in GMT time).
     * @return int day of the month in Zulu time
     */
    public int getZDay()
    {
        return this._day;
    }

    /**
     * get the hours portion of the time this value as a Zulu time (in GMT time).
     * @return int hours of time in Zulu time
     */
    public int getZHour()
    {
        return this._hour;
    }

    /**
     * get the minutes portion of the time this value as a Zulu time (in GMT time).
     * @return int minutes of time in Zulu time
     */
    public int getZMinute()
    {
        return this._minute;
    }

    /**
     * get the seconds portion of the time this value as a Zulu time (in GMT time).
     * @return int seconds of time in Zulu time
     */
    public int getZSecond()
    {
        return this._second;
    }

    /**
     * get the fraction seconds portion of the time this value as a Zulu time (in GMT time).
     * If the precision doesn't include fraction seconds this will return null.
     * @return int fractional seconds of time in Zulu time, if specified otherwise null
     */
    public BigDecimal getZFractionalSecond()
    {
        return this._fraction;
    }

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
     * a variant of the standard toString that returns the value
     * as if it had been specified in zulu time.  This is especially
     * useful for debugging as the resulting value has had the
     * local offset "applied".
     *
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
     * Prints this timestamp in Ion format.
     *
     * @param out must not be null.
     *
     * @throws IOException propagated when the {@link Appendable} throws it.
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
     * Prints this timestamp in Ion format but without the timezone.
     * Technically this method treats the timezone as if it had been
     * zulu time.
     *
     * @param out must not be null.
     *
     * @throws IOException propagated when the {@link Appendable} throws it.
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

    // TODO - check this and see if we're really getting decent values out of this
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = this._precision.hashCode();
        result = prime * result + this.dateValue().hashCode();
        result = prime * result +
                 (this._precision == Precision.FRACTION
                     ? _fraction.hashCode() : 0);
        result = prime * result + (_offset == null ? 0 : _offset.hashCode());
        return result;
    }

    /**
     * Performs a timeline comparison of the instant represented by two
     * Timestamps.
     * If the instant represented by this object precedes that of {@code t},
     * then {@code -1} is returned.
     * If {@code t} precedes this object then {@code 1} is returned.
     * If the timestamps represent the same instance on the timeline, then
     * {@code 0} is returned.
     * Note that a {@code 0} result does not imply that the two values are
     * {@link #equals}, as the timezones or precision of the two values may be
     * different.
     *
     * @param t second timestamp to compare 'this' to
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     *
     * @throws ClassCastException if {@code t} is not a {@link Timestamp}.
     * @throws NullPointerException if {@code t} is null.
     */
    public int compareTo(Object t) {
        return this.compareTo((Timestamp)t);
    }

    /**
     * Performs a timeline comparison of the instant represented by two
     * Timestamps.
     * If the instant represented by this object precedes that of {@code t},
     * then {@code -1} is returned.
     * If {@code t} precedes this object then {@code 1} is returned.
     * If the timestamps represent the same instant on the timeline, then
     * {@code 0} is returned.
     * Note that a {@code 0} result does not imply that the two values are
     * {@link #equals}, as the timezones or precision of the two values may be
     * different.
     * <p>
     * This method is provided in preference to individual methods for each of
     * the six boolean comparison operators (<, ==, >, >=, !=, <=).
     * The suggested idiom for performing these comparisons is:
     * {@code (x.compareTo(y) }<em>&lt;op></em>{@code 0)},
     * where <em>&lt;op></em> is one of the six comparison operators.
     *
     * @param t second timestamp to compare 'this' to
     * @return -1, 0, or 1 as this {@code Timestamp}
     * is less than, equal to, or greater than {@code t}.
     *
     * @throws NullPointerException if {@code t} is null.
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
     * precision, and local offset as this object.
     * <p>
     * Use the {@link #compareTo(Object)} method to compare only the point in
     * time.
     */
    @Override
    public boolean equals(Object t)
    {
        if (!(t instanceof Timestamp)) return false;
        return equals((Timestamp)t);
    }

    /**
     * Compares this {@link Timestamp} to another.
     * The result is {@code true} if and only if the parameter
     * represents the same point in time and has
     * the same precision and local offset as this object.
     * <p>
     * Use the {@link #compareTo(Timestamp)} method to compare only the point
     * in time.
     */
    public boolean equals(Timestamp t)
    {
        if (this == t) return true;
        if (t == null) return false;

        // if the precisions are not the same the values are not
        // precision doesn't matter WRT to equality
        if (this._precision != t._precision) return false;

        // if the local offset are not the same the values are not
        if (this._offset == null) {
            if (t._offset != null)  return false;
        }
        else {
            if (t._offset == null) return false;
        }

        // so now we check the actual time value
        long this_millis = this.getMillis();
        long other_millis = t.getMillis();
        if (this_millis != other_millis) return false;

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
