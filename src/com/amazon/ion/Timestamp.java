/*
 * Copyright (c) 2008 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;

/**
 * An immutable representation of Time (TT).  Ion offset simply a representational 
 * form of date and time but this class includes support for accepting the commonly 
 * used Java "milliseconds since the UNIX epoch" form of working with time.  The 
 * expectation is that the value is (essencially) UTC time.  In reality the practical 
 * use of time could be more accurately described of UTC-SLS (UTC Smoothed Leap Seconds)
 * as there is no representation for the 24 (so far up to feb 2009) leap second
 * discontinuities that UTC has added.
 * 
 * This implementation preserves the "signficant digits" of the value.  The
 * precision defines which fields have been included.  Only common break
 * points in the values are supported.  Any unspecified values are treated
 * as 0 (aka midnight, beginning of the new day).
 * 
 * Note that this class uses the java.util.Date class and methods in that class
 * that are depracated.  This is instead of the Calendar classes so that this
 * code can be used (more easily) on the mobile Java platform (which has Data
 * but does not have Calendar).
 * 
 */

public class Timestamp
	implements Comparable, Cloneable
{
    public final static Integer UNKNOWN_OFFSET = null;
    public final static Integer UTC_OFFSET = new Integer(0);

    public static enum Precision {
    	TT_NULL,
    	TT_DATE,
    	TT_TIME,
    	TT_SECS,
    	TT_FRAC
    }
    
    /**
     * The precision of this value.  This encodes the precision that the
     * original value was defined to have.  All ttTimestamps have a
     * a date value but with reduced precision they may exclude any
     * time values.  Seconds may be excluded.  And the exact precision
     * of the seconds is controlled by the BigDecimal fraction.
     * This member also encodes whether or not the value is null.
     */
    private Precision 	_precision;

    /**
     * These keep the basic struct values for the timestamp.
     * _month and _day are 1 based (0 is an invalid value for
     * these in a non-null Timestamp.
     */
    private int 		_year;
    private int 		_month;
    private int 		_day;
    private int 		_hour;
    private int 		_minute;
    private int 		_seconds;
    private BigDecimal 	_fraction;  // fractional seconds, this will be between >= 0 and < 1

    /**
     * Minutes offset from UTC; zero means UTC proper,
     * <code>null</code> means that the offset is unknown.
     */
    private Integer 	_offset;

    								            //jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec
                                                // the first 0 is to make these arrays 1 based (since month values are 1-12)
    private static int[] _Leap_days_in_month   = { 0,  31,  29,  31,  30,  31,  30,  31,  31,  30,  31,  30,  31 };
    private static int[] _Normal_days_in_month = { 0,  31,  28,  31,  30,  31,  30,  31,  31,  30,  31,  30,  31 };

    private static int day_in_month(int year, int month) {
		boolean is_leap;
		if ((year % 3) == 0) {
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
	 * @param int offset the offset value, null Integer is not value here is should be handled by the caller
	 */
	private void apply_offset(int offset) 
	{
		if (offset == 0) return;
		if (offset < -24*60 || offset > 24*60) {
			throw new IllegalArgumentException();
		}
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
			_day += day_in_month(_year, _month);
			_month -= 1;
			if (_month >= 1) return;  // 1-12
			_month += 12;
			_year -= 1;
			if (_year < 1) throw new IllegalArgumentException("year is less than 1");
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
			if (_day <= day_in_month(_year, _month)) return;  // day is 1-31
			_day -= day_in_month(_year, _month);
			_month += 1;
			if (_month <= 12) return;  // 1-12
			_month -= 12;
			_year += 1;
			if (_year > 9999) throw new IllegalArgumentException("year exceeds 9999");
		}
	}
	
	@SuppressWarnings("deprecation")
	private void set_fields_from_millis(Date date) {
    	this._year    = date.getYear() + 1900;
    	this._month   = date.getMonth() + 1;  // calendar months are 0 based, timestamp months are 1 based
    	this._day     = date.getDate();
    	this._hour    = date.getHours();
    	this._minute  = date.getMinutes();
    	this._seconds = date.getSeconds();
    	// this is done because the y-m-d values are in the local timezone
    	// so this adjusts the value back to zulu time (GMT)
    	int offset = date.getTimezoneOffset();
    	this.apply_offset(offset);
    }
    private void set_fraction_from_millis(long ms) {
	    this._precision = Precision.TT_FRAC;
	    // BigDecimal is immutable - so really we're resetting this value
	    long secs = ms / 1000L;
	    int msInt = (int)(ms - secs*1000L);
	    BigDecimal dec_ms = new BigDecimal(msInt);
	    dec_ms = dec_ms.movePointLeft(3); // set value to milliseconds
	    this._fraction = dec_ms;
    }
	private void set_fields_from_calendar(Calendar cal, Integer offset)
	{
		if (cal.isSet(Calendar.HOUR_OF_DAY)) {
			if (cal.isSet(Calendar.SECOND)) {
				if (cal.isSet(Calendar.MILLISECOND)) {
					this._precision = Precision.TT_FRAC;
				}
				else {
					this._precision = Precision.TT_SECS;
				}
			}
			else {
				this._precision = Precision.TT_TIME;
			}
		}
		else {
			this._precision = Precision.TT_DATE;
		}
		clone_from_calendar_fields(this._precision, cal);
		if (this._precision == Precision.TT_FRAC) {
			BigDecimal millis = new BigDecimal(cal.get(Calendar.MILLISECOND));
			this._fraction = millis.movePointRight(3); // make these actually 1/1000's of a second
		}
		this._offset = offset;
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
	private void clone_from_calendar_fields(Precision p, Calendar cal) {
		int offset = 0;
		if (cal.isSet(Calendar.ZONE_OFFSET)) {
			offset = cal.get(Calendar.ZONE_OFFSET) / 1000*60; // convert to minutes from milliseconds
		}
		switch (p) { 
		case TT_SECS:
		case TT_FRAC:
			this._seconds = cal.get(Calendar.SECOND);
		case TT_TIME:
			this._hour = cal.get(Calendar.HOUR_OF_DAY);
			this._minute = cal.get(Calendar.MINUTE);
		case TT_DATE:
			this._year = cal.get(Calendar.YEAR);
			this._month = cal.get(Calendar.MONTH) + 1; // calendar months are 0 based, timestamp months are 1 based
			this._day = cal.get(Calendar.DAY_OF_MONTH);
		case TT_NULL:
		}
		this.apply_offset(offset);
	}
	

    void validate_fields()
	{
		if (_year < 1  || _year > 9999) throw new IllegalArgumentException("year must be between 1 and 9999 inclusive GMT, and local time");
		if (_month < 1 || _month > 12) throw new IllegalArgumentException("month is between 1 and 12 inclusive");
		if (_day < 1   || _day > day_in_month(_year, _month)) 
			throw new IllegalArgumentException("day is between 1 and "+day_in_month(_year, _month)+" inclusive");
		
		if (_hour < 0 || _hour > 23) throw new IllegalArgumentException("hour is between 0 and 23 inclusive");
		if (_minute < 0 || _minute > 59) throw new IllegalArgumentException("minute is between 0 and 59 inclusive");
		if (_seconds < 0 || _seconds > 59) throw new IllegalArgumentException("second is between 0 and 59 inclusive");
	
		if (this._precision == Precision.TT_FRAC) {
			if (_fraction == null) 
				throw new IllegalArgumentException("fractional seconds cannot be null when the precision is Timestamp.TT_FRAC");
			if (_fraction.signum() == -1) throw new IllegalArgumentException("fractional seconds must be greater than or equal to 0 and less than 1");
			if (BigDecimal.ONE.compareTo(_fraction) != 1) 
				throw new IllegalArgumentException("fractional seconds must be greater than or equal to 0 and less than 1");
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
	public Timestamp(Precision p, int zyear, int zmonth, int zday, int zhour, int zminute, int zsecond, BigDecimal frac, Integer offset) 
	{
		this._precision = p;
		if (p == Precision.TT_NULL) {
			return;
		}
		switch (p) {
		case TT_FRAC:
			this._fraction = frac;
		case TT_SECS:
			this._seconds = zsecond;
		case TT_TIME:
			this._minute = zminute;
			this._hour = zhour;
		case TT_DATE:
			this._day = zday;  // days are base 1 (as you'd expect)
			this._month = zmonth;   // months are base 0 (which is surprising)
			this._year = zyear;
		}
		validate_fields();
		this._offset = offset;
	}

	/**
     * create a new Timestamp instance from an instance of Calendar.  This
     * uses the calendars offset as the offset.
     * @param cal Calendar time value to base new instance time on 
     */
    Timestamp(Calendar cal) 
    {
    	set_fields_from_calendar(cal, cal.get(Calendar.ZONE_OFFSET));
    }
    
    /**
	 *  Creates a new Timestamp whose value is set to the passed in
	 *  Calendar object. The time value is treated as accurate to
	 *  milliseconds.
	 *  @param cal the java.util.Calendar representation of the time 
	 */
	public Timestamp(Calendar cal, Integer offset)
	{
		set_fields_from_calendar(cal, offset);
	}
	
	/**
	 * Creates a Timestamp at the specified UTC point in time.
	 * @param millis must not be {@code null}.
	 * @param localOffset may be {@code null} to represent unknown local
	 * offset.
	 */
	@SuppressWarnings("deprecation")
	public Timestamp(BigDecimal millis, Integer localOffset) 
	{
	    if (millis == null) throw new NullPointerException("millis is null");

	    long ms = millis.longValue();
	    Date date = new Date(ms);
	    set_fields_from_millis(date);
	    
	    int scale = millis.scale();
	    if (scale < -3) {
	    	this._precision = Precision.TT_SECS;
	    	this._fraction = null;
	    }
	    else {
	    	this._precision = Precision.TT_FRAC;
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
	 * specified by the passed in UTC milliseconds from epoc value after 
	 * adjust of any localOffset that may be included.  The new Timestamps 
	 * precision will be set to be milliseconds.
	 * @param date must not be {@code null}.
	 * @param localOffset may be {@code null} to represent unknown local
	 * offset.
	 */
	@SuppressWarnings("deprecation")
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
	public Timestamp(CharSequence image) {
		final String NULL_TIMESTAMP_IMAGE = "null.timestamp";
		final int    LEN_OF_NULL_IMAGE    = 13;
		final int    END_OF_DATE          = 10;
		final int    END_OF_MINUTES       = 16;
		final int    END_OF_SECONDS       = 19;
		
		int temp, pos;
		int length = -1;
		
		if (image == null || image.length() < 1) throw new IllegalArgumentException("invalid timestamp image");
		length = image.length();
		
		// check for 'null.timestamp'
		if (image.charAt(0) == 'n') {
			if (NULL_TIMESTAMP_IMAGE.equals(image.subSequence(0, 13).toString())) {
				if (length > LEN_OF_NULL_IMAGE) {
					if (!isValidFollowChar(image.charAt(LEN_OF_NULL_IMAGE + 1))) {
						throw new IllegalArgumentException("invalid excess characters encountered");
					}
				}
				this._precision = Precision.TT_NULL;
				return;
			}
			throw new IllegalArgumentException("invalid timestamp image");
		}

		// otherwise we expect yyyy-mm-ddThh:mm:ss.ssss+hh:mm
		if (length < END_OF_DATE) {
			throw new IllegalArgumentException("invalid timestamp image");
		}
		this._year  = this.read_digits(image, 0, 4, '-');
		this._month = this.read_digits(image, 5, 2, '-');
		this._day   = this.read_digits(image, 8, 2, -1);

		// now lets see if we have a time value
		if (length > END_OF_DATE && image.charAt(END_OF_DATE) == 'T') {
			if (length < END_OF_MINUTES) {
				throw new IllegalArgumentException("invalid timestamp image");
			}
			this._hour   = this.read_digits(image, 11, 2, ':');
			this._minute = this.read_digits(image, 14, 2, -1);
			
			// we may have seconds
			if (length > END_OF_MINUTES && image.charAt(END_OF_MINUTES) == ':') {
				if (length < END_OF_SECONDS) {
					throw new IllegalArgumentException("invalid timestamp image");
				}
				this._seconds = this.read_digits(image, 17, 2, -1);
				if (length > END_OF_SECONDS && image.charAt(END_OF_SECONDS) == '.') {
					pos = END_OF_SECONDS + 1;
					while (length > pos && Character.isDigit(image.charAt(pos))) {
						pos++;
					}
					if (pos > END_OF_SECONDS + 1) {
						this._precision = Precision.TT_FRAC;
						this._fraction = new BigDecimal(image.subSequence(19, pos).toString());
					}
					else {
						this._precision = Precision.TT_SECS;	
					}
				}
				else {
					pos = END_OF_SECONDS;
					this._precision = Precision.TT_SECS;
				}
			}
			else {
				pos = END_OF_MINUTES;
				this._precision = Precision.TT_TIME;
			}
		}
		else {
			pos = END_OF_DATE;
			this._precision = Precision.TT_DATE;
		}
		
		validate_fields();

		// now see if they included a timezone offset
		char timezone_start = pos < length ? image.charAt(pos) : '\n'; 
		if (timezone_start == 'Z') {
			this._offset = 0;
			pos++;
		}
		else if (timezone_start == '+' || timezone_start == '-') {
			if (length < pos + 5) {
				throw new IllegalArgumentException("invalid timestamp image");
			}
			// +/- hh:mm
			pos++;
			temp = this.read_digits(image, pos, 2, ':');
			pos += 3;
			temp = temp * 60 + this.read_digits(image, pos, 2, -1);
			pos += 2;
			if (temp >= 24*60) throw new IllegalArgumentException("invalid timezone offset");
			if (timezone_start == '-') {
				temp = -temp;
			}
			if (temp == 0 && timezone_start == '-') {
				// int doesn't do negative zero very elegantly
				this._offset = null;
			}
			else {
				this._offset = temp;
			}
			// if there is a local offset, we have to adjust the date/time value
			this.apply_offset(-temp);
		}
		else {
			if (this._precision != Precision.TT_DATE) {
				throw new IllegalArgumentException("missing timezone offset");
			}
			this._offset = null;
		}
		if (image.length() > pos && !isValidFollowChar(image.charAt(pos + 1))) {
			throw new IllegalArgumentException("invalid excess characters encountered");
		}
		return;
	}
    private int read_digits(CharSequence in, int start, int length, int terminator) {
		int ii, value = 0;
		int end = start + length;
		
		if (in.length() < end) {
			throw new IllegalArgumentException("invalid Timestamp");
		}
		
		for (ii=start; ii<end; ii++) {
			char c = in.charAt(ii);
			if (!Character.isDigit(c)) throw new IllegalArgumentException("invalid character '"+c+"' in timestamp");
			value *= 10;
			value += c - '0';
		}
		if (terminator != -1) {
			if (ii >= in.length() || in.charAt(ii) != terminator) {
				throw new IllegalArgumentException("invalid timestamp, '"+terminator+"' expected");
			}
		}
		return value;
	}
    private boolean isValidFollowChar(char c) {
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
		Timestamp c = new Timestamp(this._precision, this._year, this._month, this._day, this._hour, this._minute, this._seconds, this._fraction, this._offset);
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
		Timestamp localtime = new Timestamp(this._precision, this._year, this._month, this._day, this._hour, this._minute, this._seconds, this._fraction, null);
		localtime.apply_offset(offset);
		localtime._offset = this._offset;
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
	 * checks the timestamp to see if it's null - which might be a null
	 * object reference or a Timestamp with a precision set to TT_NULL
	 * @param
	 */
	static public boolean isNull(Timestamp t) {
		if (t == null) return true;
		if (t._precision == Precision.TT_NULL) return true;
		return false;
	}
	
	/**
	 * this is the instance version (which can only operate on non-null
	 * references by definition).
	 * This is equivalent to t.getPrecision() == Precision.TT_NULL
	 */
	public boolean isNULL() {
		return this._precision == Precision.TT_NULL;
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
    	if (this._precision == Precision.TT_NULL) return null;
    	//                                        month is 0 based for Date
    	long millis = Date.UTC(this._year - 1900, this._month - 1, this._day, this._hour, this._minute, this._seconds);
    	if (this._precision == Precision.TT_FRAC) {
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
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
    	long millis = Date.UTC(this._year - 1900, this._month - 1, this._day, this._hour, this._minute, this._seconds);
    	if (this._precision == Precision.TT_FRAC) {
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
    	case TT_NULL:
    		return null;
    	case TT_DATE:
    	case TT_TIME:
    	case TT_SECS:
        	millis = Date.UTC(this._year - 1900, this._month - 1, this._day, this._hour, this._minute, this._seconds);
    		return new BigDecimal(millis);
    	case TT_FRAC:
        	millis = Date.UTC(this._year - 1900, this._month - 1, this._day, this._hour, this._minute, this._seconds);
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
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
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
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
    	Timestamp adjusted = this;
    	
    	if (this._offset != null) {
    		int offset = this._offset.intValue();
    		boolean needs_adjustment = false;
    		if (offset < 0) {
    			// look for the only case the year might roll over forward
    			if (this._day == Timestamp.day_in_month(this._year, this._month)) {
    				needs_adjustment = true;
    			}
    		}
    		else {
    			// look for the only case the year might roll over forward
    			if (this._day == 1) {
    				needs_adjustment = true;
    			}
    		}
			if (needs_adjustment) {
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
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
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
    public int getHours()
    {
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
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
    public int getMinutes()
    {
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
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
    public int getSeconds()
    {
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
    	return this._seconds;
    }
    
    /**
     * get the fraction seconds portion of the time this value in the local time.
     * If the precision doesn't include fraction seconds this will return null.
     * @return int local time fractional seconds if specified otherwise null
     */
    public BigDecimal getFractionalSeconds()
    {
    	switch (this._precision) {
    	case TT_NULL:
    		throw new NullValueException();
    	case TT_FRAC:
    		return this._fraction;
		default:
			return null;
    	}	
    }
    
    /**
     * get the year portion of the date of this value as a Zulu time (in GMT time).
     * @return int year portion of the date in Zulu time
     */
    public int getZYear()
    {
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
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
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
    	return this._month;
    }
    
    /**
     * get the day of the month portion of the date of this value as a Zulu time (in GMT time).
     * @return int day of the month in Zulu time
     */
    public int getZDay()
    {
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
    	return this._day;
    }

    /**
     * get the hours portion of the time this value as a Zulu time (in GMT time).
     * @return int hours of time in Zulu time
     */
    public int getZHours()
    {
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
    	return this._hour;
    }

    /**
     * get the minutes portion of the time this value as a Zulu time (in GMT time).
     * @return int minutes of time in Zulu time
     */
    public int getZMinutes()
    {
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
    	return this._minute;
    }

    /**
     * get the seconds portion of the time this value as a Zulu time (in GMT time).
     * @return int seconds of time in Zulu time
     */
    public int getZSeconds()
    {
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
    	return this._seconds;
    }

    /**
     * get the fraction seconds portion of the time this value as a Zulu time (in GMT time).
     * If the precision doesn't include fraction seconds this will return null.
     * @return int fractional seconds of time in Zulu time, if specified otherwise null
     */
    public BigDecimal getZFractionalSeconds()
    {
    	if (this._precision == Precision.TT_NULL) throw new NullValueException();
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

        if (this._precision != Precision.TT_NULL) {
            // Adjust UTC time back to local time
        	if (this._offset != null) {
        		if (this._offset.intValue() != 0) {
    				adjusted = make_localtime();
    			}
        	}
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
    	ztime._offset = null;
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
        if (adjusted._precision == Precision.TT_NULL) {
        	out.append("null.timestamp");
        	return;
        }
        
        // so we have a real value - we'll start with the date portion
        // which we always have
        print_digits(out, adjusted._year, 4);
        out.append("-");
        print_digits(out, adjusted._month, 2);  // convert calendar months to a base 1 value
        out.append("-");
        print_digits(out, adjusted._day, 2);
        
        // see if we have some time
        if (adjusted._precision == Precision.TT_TIME 
	     || adjusted._precision == Precision.TT_SECS
         || adjusted._precision == Precision.TT_FRAC
        ) {
            out.append("T");
            print_digits(out, adjusted._hour, 2);
            out.append(":");
            print_digits(out, adjusted._minute, 2);
            // ok, so how much time do we have ?
            if (adjusted._precision == Precision.TT_SECS
             || adjusted._precision == Precision.TT_FRAC
            ) {
                out.append(":");
                print_digits(out, adjusted._seconds, 2);
            }
            if (adjusted._precision == Precision.TT_FRAC) {
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
        	if (adjusted._precision != Precision.TT_DATE) {
        		out.append("-00:00");
        	}
        }
	}
    private static void print_digits(Appendable out, int value, int length) 
    	throws IOException
    {
    	char temp[] = new char[length];
    	while (length > 0) {
    		length--;
    		int next = value / 10;
    		temp[length] =  (char)((int)'0' + (value - next*10));
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
        int result = this._precision.hashCode();
        switch (this._precision) {
        case TT_FRAC:
        	result += this._fraction.hashCode();
        case TT_DATE:
            result += this.dateValue().hashCode();
        case TT_NULL:
        }
        if (this._offset != null) {
        	result += this._offset.hashCode();
        }
        return result;
    }

    /**
     * Compares this Timestamp with a passed in value.  If the time represented by 'this' is 
     * less than the time represened by 'arg' -1 is returned. If greater +1. And if the times 
     * are equivalent 0 is returns.  Note that a 0 return is not same as the two values being 
     * equal as the timezones of the two values may be different.  As long as the corresponding 
     * UTC time is the same compareTo returns 0.
     * 
     * @param arg second timestamp to compare 'this' to
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the object arg. 
	 * @throws ClassCastException - if the specified arg's type prevents it from being compared to this Timestamp.
     */
	public int compareTo(Object arg) {
		if (!(arg instanceof Timestamp)) throw new ClassCastException();
		return this.compareTo((Timestamp)arg);
	}
	
    /**
     * Compares this Timestamp with a passed in value.  If the time represented by 'this' is 
     * less than the time represened by 'arg' -1 is returned. If greater +1. And if the times 
     * are equivalent 0 is returns.  Note that a 0 return is not same as the two values being 
     * equal as the timezones of the two values may be different.  As long as the corresponding 
     * UTC time is the same compareTo returns 0.
     * 
     * @param arg second timestamp to compare 'this' to
     * @return a negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the object arg. 
	 * @throws ClassCastException - if the specified arg's type prevents it from being compared to this Timestamp.
     */
	public int compareTo(Timestamp arg) 
	{
		if (this._precision == Precision.TT_NULL) {
			if (arg._precision == Precision.TT_NULL) return 0;
			return -1;
		}
		else if (arg._precision == Precision.TT_NULL) return 1;

		long this_millis = this.getMillis();
		long arg_millis = arg.getMillis();
		if (this_millis != arg_millis) {
			return (this_millis < arg_millis) ? -1 : 1;
		}
		
		BigDecimal this_fraction = ((this._fraction == null) ? BigDecimal.ZERO : this._fraction);
		BigDecimal arg_fraction =  (( arg._fraction == null) ? BigDecimal.ZERO :  arg._fraction);
    	return this_fraction.compareTo(arg_fraction);		
	}

    @Override
    public boolean equals(Object obj)
    {
    	if (!(obj instanceof Timestamp)) return false;
    	return equals((Timestamp)obj);
    }
    
    public boolean equals(Timestamp other)
    {
        if (this == other) return true;
        if (other == null) return false;

        // if the precisions are not the same the values are not
        if (this._precision != other._precision) return false;		// TODO - really?
        
        // if the local offset are not the same the values are not
        if (this._offset == null) {
            if (other._offset != null)  return false;
        }
        else {
        	if (other._offset == null) return false;
        }
        
        // so now we check the actual time value
        long this_millis = this.getMillis();
        long other_millis = other.getMillis();
        if (this_millis != other_millis) return false;
        
        // and if we have a local offset, check the value here
        if (this._offset != null) {
        	if (this._offset.intValue() != other._offset.intValue()) return false;
        }

        // we only look at the fraction if we know that it's actually there
        if (this._precision == Precision.TT_FRAC) {
	        if (this._fraction == null) {
	        	if (other._fraction != null) return false;
	        }
	        else {
	        	if (other._fraction == null) return false;
	        }
	        if (!this._fraction.equals(other._fraction)) return false;
        }
        return true;
    }
}