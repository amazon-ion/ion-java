/*
 * Copyright (c) 2007 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;



/**
 * Validates Ion date parsing, specified as per W3C but with requiring
 * at least year-month-day.
 *
 * @see <a href="http://www.w3.org/TR/NOTE-datetime">the W3C datetime note</a>
 */
public class TimestampTest
    extends IonTestCase
{
    /**
     * The UTC TimeZone.
     *
     * TODO determine if this is well-defined.
     */
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final TimeZone PST = TimeZone.getTimeZone("PST");


    private static final Calendar MAGIC_DAY;
    static {
        MAGIC_DAY = makeUtcCalendar();
        // month is zero-based!
        MAGIC_DAY.set(1969, 01, 23);
    }


    public static Calendar makeUtcCalendar()
    {
        Calendar cal = Calendar.getInstance(UTC);
        cal.setTimeInMillis(0);        // clear all fields, else they are "now"
        return cal;
    }

    /**
     *
     * @param year
     * @param month is zero-based!
     * @param dayOfMonth
     * @return a new Calendar
     */
    public static Calendar makeCalendarDate(int year, int month, int dayOfMonth)
    {
        Calendar cal = makeUtcCalendar();
        cal.set(year, month, dayOfMonth);
        return cal;
    }

    private void checkTimestamp(Date expected, IonTimestamp actual)
    {
        Date found = actual.dateValue();
        assertNotNull("date is null", found);
        // Ensure that dateValue() returns a new instance each call!
        assertNotSame(found, actual.dateValue());

        assertEquals(found.getTime(), actual.getMillis());

        if (expected.getTime() != found.getTime())
        {
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
            format.setTimeZone(UTC);
            fail("expected " + format.format(expected.getTime())
                 + " found " + format.format(found));
        }
    }

    private void checkTimestamp(Calendar expected, IonTimestamp actual)
    {
        checkTimestamp(expected.getTime(), actual);

        long expectedOffsetMillis = expected.getTimeZone().getRawOffset();
        Integer actualOffsetMinutes = actual.getLocalOffset();
        if (expectedOffsetMillis == 0)
        {
            assertTrue(actualOffsetMinutes == null
                       || actualOffsetMinutes.intValue() == 0);
        }
        else
        {
            int actualOffsetMillis = actualOffsetMinutes * 60 * 1000;
            assertEquals(expectedOffsetMillis, actualOffsetMillis);
        }
    }


    private void checkMagicDay(String text)
    {
        checkMagicDay(null, text);
    }

    private void checkMagicDay(TimeZone tz, String text)
    {
        IonTimestamp value = (IonTimestamp) oneValue(text);
        Calendar magicDay = makeUtcCalendar();
        // month is zero-based!
        magicDay.set(1969, 01, 23);

        if (tz != null)
        {
            magicDay.setTimeZone(tz);
        }

        checkTimestamp(magicDay, value);
    }

    public void checkNullTimestamp(IonTimestamp value)
    {
        assertTrue(value.isNullValue());
        assertNull(value.dateValue());
        try
        {
            value.getLocalOffset();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }
    }

    public void modifyTimestamp(IonTimestamp value)
    {
        Integer offset = (value.isNullValue() ? null : value.getLocalOffset());

        Date now = new Date();
        Calendar cal = Calendar.getInstance(UTC);
        cal.setTime(now);
        cal.roll(Calendar.DATE, false);
        Date yesterday = cal.getTime();

        value.setTime(now);
        checkTimestamp(now, value);
        assertEquals(offset, value.getLocalOffset());

        value.setLocalOffset(13);
        checkTimestamp(now, value);
        assertEquals(13, value.getLocalOffset().intValue());

        value.setLocalOffset(new Integer(-60));
        checkTimestamp(now, value);
        assertEquals(-60, value.getLocalOffset().intValue());

        value.setTime(yesterday);
        checkTimestamp(yesterday, value);
        assertEquals(-60, value.getLocalOffset().intValue());

        value.addTypeAnnotation("a");
        checkTimestamp(yesterday, value);
        assertEquals(-60, value.getLocalOffset().intValue());

        value.setMillis(now.getTime());
        checkTimestamp(now, value);
        assertEquals(-60, value.getLocalOffset().intValue());

        value.setLocalOffset(null);
        checkTimestamp(now, value);
        assertEquals(null, value.getLocalOffset());

        value.setTime(null);
        checkNullTimestamp(value);
    }


    // ========================================================================

    public void testFactoryNullTimestamp()
    {
        IonTimestamp value = system().newTimestamp();
        checkNullTimestamp(value);
        modifyTimestamp(value);
    }

    public void testTextNullTimestamp()
    {
        IonTimestamp value = (IonTimestamp) oneValue("null.timestamp");
        checkNullTimestamp(value);
        modifyTimestamp(value);
    }


    public void testBadDates()
    {
        badValue("1969-");
        badValue("1696-02");
        badValue("1969-02-");
        badValue("1969-02-23t00:00Z");      // bad separator
        badValue("1969-02-23T00:00z");      // bad TZD
    }

    public void testDateWithSlashes()
    {
        badValue("2007/02/21");
    }


    public void testMillis()
    {
        IonTimestamp value =
            (IonTimestamp) oneValue("2001-01-01T12:34:56.789Z");

        Calendar cal = makeUtcCalendar();
        cal.set(2001, 00, 01, 12, 34, 56);
        cal.set(Calendar.MILLISECOND, 789);

        checkTimestamp(cal, value);
    }

    public void testBareDate()
    {
        checkMagicDay("1969-02-23");
    }

    public void testDateWithMinutes()
    {
        checkMagicDay("1969-02-23T00:00Z");
    }

    public void testDateWithMillis()
    {
        checkMagicDay("1969-02-23T00:00:00.000Z");
    }

    public void testDateWithMinutesAndPosTzd()
    {
        checkMagicDay("1969-02-23T00:00+00:00");
    }

    public void testDateWithMillisAndPosTzd()
    {
        checkMagicDay("1969-02-23T00:00:00.00+00:00");
    }


    public void testPrecision()
    {
        IonTimestamp t1 = (IonTimestamp) oneValue("2007-08-28T16:37:24Z");
        IonTimestamp t2 = (IonTimestamp) oneValue("2007-08-28T16:37:24.0Z");
        IonTimestamp t3 = (IonTimestamp) oneValue("2007-08-28T16:37:24.00Z");
        IonTimestamp t4 = (IonTimestamp) oneValue("2007-08-28T16:37:24.000Z");
        // TODO verify structural inequality.
    }


    public void testDateWithNormalTzd()
    {
        IonTimestamp value = (IonTimestamp) oneValue("1969-02-22T16:00-08:00");
        Calendar magicDay = makeUtcCalendar();
        // month is zero-based!
        magicDay.set(1969, 01, 22, 16, 0);
        magicDay.setTimeZone(PST);

        checkTimestamp(magicDay, value);
    }

    public void testdateWithOddTzd()
    {
        IonTimestamp value = (IonTimestamp) oneValue("1969-02-23T01:15:00.00+01:15");
        Calendar magicDay = makeUtcCalendar();
        // month is zero-based!
        magicDay.set(1969, 01, 23);

        checkTimestamp(magicDay.getTime(), value);
        assertEquals(75, value.getLocalOffset().intValue());
    }

    public void testLocalOffset()
    {
        IonTimestamp value = (IonTimestamp) oneValue("1969-02-23T00:00+01:23");
        assertEquals(83, value.getLocalOffset().intValue());

        value = (IonTimestamp) oneValue("2007-05-08T05:17-12:07");
        assertEquals(-727, value.getLocalOffset().intValue());

        value = (IonTimestamp) oneValue("2007-05-08T05:17Z");
        assertEquals(0, value.getLocalOffset().intValue());
    }

    public void testUnknownLocalOffset()
    {
        IonTimestamp value = (IonTimestamp) oneValue("2007-05-08T05:17-00:00");
        assertEquals(null, value.getLocalOffset());
    }

    public void testDateWithZ()
    {
        badValue("1696-02-23Z");
    }

    public void testTruncatedOffset()
    {
        badValue("2004-12-11T12:10:11+8");
    }

    public void testTruncatedSeconds()
    {
        badValue("2004-12-11T12:10:1Z");
        badValue("2004-12-11T12:10:1-08:00");
        badValue("2004-12-11T12:10:1+08:00");
    }

    public void testYearZero()
    {
        badValue("0000-01-01");
        badValue("0000-12-31");
    }

    public void testNegativeYear()
    {
        badValue("-2000-01-01");
    }

    public void testPositiveYear()
    {
        badValue("+2000-01-01");
    }

    public void testYearOne()
    {
        IonTimestamp value = (IonTimestamp) oneValue("0001-01-01");
        checkTimestamp(makeCalendarDate(1, 0, 1), value);

        value = (IonTimestamp) oneValue("0001-12-31");
        checkTimestamp(makeCalendarDate(1, 11, 31), value);
    }


    public void testBadSetLocalOffset()
    {
        IonTimestamp value = system().newTimestamp();

        try {
            value.setLocalOffset(0);
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        try {
            value.setLocalOffset(new Integer(60));
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }

        try {
            value.setLocalOffset(null);
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }
    }

    public void testTimestampsFromSuite()
        throws Exception
    {
        Iterable<IonValue> values = readTestFile("good/timestamps.ion");
        // File is a sequence of many timestamp values.

        for (IonValue value : values)
        {
            String v = value.toString();
            assertTrue(value instanceof IonTimestamp || v.equals("just some crap that can't be"));
        }
    }
}
