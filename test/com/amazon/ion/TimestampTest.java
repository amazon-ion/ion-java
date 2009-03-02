/* Copyright (c) 2007-2009 Amazon.com, Inc.  All rights reserved. */

package com.amazon.ion;

import static com.amazon.ion.Timestamp.UNKNOWN_OFFSET;
import static com.amazon.ion.Timestamp.UTC_OFFSET;

import com.amazon.ion.Timestamp.Precision;
import java.math.BigDecimal;
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


    public static Calendar makeUtcCalendar()
    {
        Calendar cal = Calendar.getInstance(UTC);
        cal.setTimeInMillis(0);        // clear all fields, else they are "now"
        return cal;
    }


    public IonTimestamp parse(String text)
    {
        return (IonTimestamp) oneValue(text);
    }

    private void checkTimestamp(Date expected, IonTimestamp actual)
    {
        assertSame(IonType.TIMESTAMP, actual.getType());
        Date found = actual.dateValue();
        assertNotNull("date is null", found);

        // Ensure that dateValue() returns a new instance each call!
        assertNotSame(found, actual.dateValue());

        assertEquals(expected, found);
        assertEquals(expected.getTime(), actual.getMillis());

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


    private void checkFields(int expectedYear, int expectedMonth, int expectedDay,
                             int expectedHour, int expectedMinute,
                             int expectedSecond,
                             BigDecimal expectedFraction,
                             Integer expectedOffset,
                             Timestamp ts)
    {
        assertEquals(expectedYear,     ts.getYear());
        assertEquals(expectedMonth,    ts.getMonth());
        assertEquals(expectedDay,      ts.getDay());
        assertEquals(expectedHour,     ts.getHour());
        assertEquals(expectedMinute,   ts.getMinute());
        assertEquals(expectedSecond,   ts.getSecond());
        assertEquals(expectedFraction, ts.getFractionalSecond());
        assertEquals(expectedOffset,   ts.getLocalOffset());
    }

    private void checkTime(int expectedYear, int expectedMonth, int expectedDay,
                           String ionText)
    {
        String expectedText = ionText;
        Timestamp ts = new Timestamp(expectedYear, expectedMonth, expectedDay);
        checkFields(expectedYear, expectedMonth, expectedDay, 0, 0, 0, null,
                    UNKNOWN_OFFSET, ts);
        assertEquals(Precision.DATE, ts.getPrecision());
        assertEquals(expectedText, ts.toString());

        checkTime(expectedYear, expectedMonth, expectedDay,
                  0, 0, 0, null,
                  UNKNOWN_OFFSET, expectedText, ionText);
    }


    private void checkTime(int expectedYear, int expectedMonth, int expectedDay,
                           int expectedHour, int expectedMinute,
                           int expectedSecond,
                           BigDecimal expectedFraction,
                           Integer expectedOffset,
                           String ionText)
    {
        String expectedText = ionText;
        checkTime(expectedYear, expectedMonth, expectedDay,
                    expectedHour, expectedMinute, expectedSecond, expectedFraction,
                    expectedOffset, expectedText, ionText);
    }

    private void checkTime(int expectedYear, int expectedMonth, int expectedDay,
                           int expectedHour, int expectedMinute,
                           int expectedSecond,
                           BigDecimal expectedFraction,
                           Integer expectedOffset,
                           String expectedText,
                           String ionText)
    {
        IonTimestamp actual = parse(ionText);
        checkFields(expectedYear, expectedMonth, expectedDay,
                    expectedHour, expectedMinute, expectedSecond, expectedFraction,
                    expectedOffset, actual);

        assertEquals(expectedText, actual.toString());
        assertEquals(expectedText, actual.timestampValue().toString());

        Timestamp ts1 = Timestamp.valueOf(expectedText);
        Timestamp ts2 = Timestamp.valueOf(ionText);

        assertEquals(ts1, ts2);
        assertEquals(ts1, actual.timestampValue());
    }

    private void checkFields(int expectedYear, int expectedMonth, int expectedDay,
                             int expectedHour, int expectedMinute,
                             int expectedSecond,
                             BigDecimal expectedFraction,
                             Integer expectedOffset,
                             IonTimestamp actual)
    {
        assertFalse(actual.isNullValue());
        assertEquals(expectedOffset, actual.getLocalOffset());

        Timestamp ts = actual.timestampValue();
        checkFields(expectedYear, expectedMonth, expectedDay,
                    expectedHour, expectedMinute, expectedSecond,
                    expectedFraction, expectedOffset,
                    ts);

        Calendar cal = makeUtcCalendar();
        cal.set(expectedYear, expectedMonth - 1, expectedDay,
                expectedHour, expectedMinute, expectedSecond);
        if (expectedFraction != null) {
            int millis = expectedFraction.movePointRight(3).intValue();
            cal.set(Calendar.MILLISECOND, millis);
        }
        if (expectedOffset != null) {
            int rawOffset = expectedOffset.intValue();
            cal.add(Calendar.MINUTE, -rawOffset);

            boolean neg = (rawOffset < 0);
            if (neg) rawOffset = -rawOffset;

            int hours = rawOffset / 60;
            int minutes = rawOffset % 60;

            String tzId =
                (neg?"GMT-":"GMT+") + hours + (minutes<10?":0":":") + minutes;
            TimeZone tz = TimeZone.getTimeZone(tzId);
            cal.setTimeZone(tz);
        }

        checkTimestamp(cal, actual);
    }


    public void checkNullTimestamp(IonTimestamp value)
    {
        assertSame(IonType.TIMESTAMP, value.getType());
        assertTrue(value.isNullValue());
        assertNull(value.dateValue());
        try
        {
            value.getLocalOffset();
            fail("Expected NullValueException");
        }
        catch (NullValueException e) { }
        assertNull(value.timestampValue());
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
        IonTimestamp value = system().newNullTimestamp();
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
        checkTime(2001, 1, 1, 12, 34, 56, new BigDecimal(".789"),
                  UTC_OFFSET,
                  "2001-01-01T12:34:56.789Z");
    }

    public void testBareDate()
    {
        checkTime(1969, 02, 23,
                  "1969-02-23");
    }

    public void testDateWithMinutes()
    {
        checkTime(1969, 02, 23, 0, 0, 0, null, UTC_OFFSET,
                  "1969-02-23T00:00Z");
    }

    public void testDateWithSeconds()
    {
        checkTime(1969, 02, 23, 0, 0, 0, null, UTC_OFFSET,
                  "1969-02-23T00:00:00Z");
    }

    public void testDateWithMillis()
    {
        checkTime(1969, 02, 23, 0, 0, 0, new BigDecimal("0.000"), UTC_OFFSET,
                  "1969-02-23T00:00:00.000Z");
    }

    public void testDateWithMinutesAndPosTzd()
    {
        checkTime(1969, 02, 23, 0, 0, 0, null, UTC_OFFSET,
                  "1969-02-23T00:00Z",
                  "1969-02-23T00:00+00:00");
    }

    public void testDateWithMillisAndPosTzd()
    {
        checkTime(1969, 02, 23, 0, 0, 0, new BigDecimal("0.00"), UTC_OFFSET,
                  "1969-02-23T00:00:00.00Z",
                  "1969-02-23T00:00:00.00+00:00");
    }


    public void checkCanonicalText(String text)
    {
        IonValue value = oneValue(text);
        String printed = value.toString();
        assertEquals(text, printed);
    }

    public void testPrecision()
    {
        checkCanonicalText("2007-08-28");
        checkCanonicalText("2007-08-28T16:37:24Z");
        checkCanonicalText("2007-08-28T16:37:24.0Z");
        checkCanonicalText("2007-08-28T16:37:24.00Z");
        checkCanonicalText("2007-08-28T16:37:24.000Z");
        checkCanonicalText("2007-08-28T16:37:24.0000Z");
    }


    public void testDateWithNormalTzd()
    {
        checkTime(1969, 02, 22, 16, 0, 0, null, new Integer(-480),
                  "1969-02-22T16:00-08:00");
    }

    public void testdateWithOddTzd()
    {
        checkTime(1969, 02, 23, 1, 15, 0, new BigDecimal("0.00"), new Integer(75),
                  "1969-02-23T01:15:00.00+01:15");
    }

    public void testLocalOffset()
    {
        IonTimestamp value = (IonTimestamp) oneValue("1969-02-23T00:00+01:23");
        assertEquals(83, value.getLocalOffset().intValue());

        checkTime(1969, 02, 23, 0, 0, 0, null, new Integer(83),
                  "1969-02-23T00:00+01:23");

        value = (IonTimestamp) oneValue("2007-05-08T05:17-12:07");
        assertEquals(-727, value.getLocalOffset().intValue());

        checkTime(2007, 5, 8, 5, 17, 0, null, new Integer(-727),
                  "2007-05-08T05:17-12:07");

        value = (IonTimestamp) oneValue("2007-05-08T05:17Z");
        assertEquals(0, value.getLocalOffset().intValue());

        checkTime(2007, 5, 8, 5, 17, 0, null, new Integer(0),
                  "2007-05-08T05:17Z");

    }

    public void testUnknownLocalOffset()
    {
        IonTimestamp value = (IonTimestamp) oneValue("2007-05-08T05:17-00:00");
        assertEquals(null, value.getLocalOffset());

        checkTime(2007, 5, 8, 5, 17, 0, null, Timestamp.UNKNOWN_OFFSET,
                  "2007-05-08T05:17-00:00");
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
        badValue("[ -2000-01-01 ]");
    }

    public void testPositiveYear()
    {
        badValue("[ +2000-01-01 ]");
    }

    public void testYearOne()
    {
        checkTime(1, 1, 1, 0, 0, 0, null, Timestamp.UNKNOWN_OFFSET,
                  "0001-01-01");
        checkTime(1, 12, 31, 0, 0, 0, null, Timestamp.UNKNOWN_OFFSET,
                  "0001-12-31");
    }


    public void testBadSetLocalOffset()
    {
        IonTimestamp value = system().newNullTimestamp();

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

        int count = 0;

        for (IonValue value : values)
        {
        	count++;
            String v = value.toString();
            assertTrue(value instanceof IonTimestamp || v.equals("just some crap that can't be"));
        }
    }


    public void testTimestampClone()
        throws Exception
    {
        testSimpleClone("null.timestamp");
//        testSimpleClone("2008-07-11");  // FIXME Date doesn't round-trip
        testSimpleClone("2008-07-11T14:49:26.000-07:00");
    }
}
