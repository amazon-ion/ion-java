// Copyright (c) 2007-2013 Amazon.com, Inc.  All rights reserved.
package com.amazon.ion;

import static com.amazon.ion.Decimal.NEGATIVE_ZERO;
import static com.amazon.ion.Decimal.negativeZero;
import static com.amazon.ion.Timestamp.UNKNOWN_OFFSET;
import static com.amazon.ion.Timestamp.UTC_OFFSET;
import static com.amazon.ion.Timestamp.Precision.DAY;
import static com.amazon.ion.Timestamp.Precision.FRACTION;
import static com.amazon.ion.Timestamp.Precision.MINUTE;
import static com.amazon.ion.Timestamp.Precision.MONTH;
import static com.amazon.ion.Timestamp.Precision.SECOND;
import static com.amazon.ion.Timestamp.Precision.YEAR;
import static com.amazon.ion.impl._Private_Utils.UTC;

import com.amazon.ion.Timestamp.Precision;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.junit.Test;



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
     * Earliest Ion timestamp possible, that is, "0001-01-01".
     *
     * @see <a href="https://w.amazon.com/index.php/Ion#Timestamps">Ion wiki page</a>
     */
    private static final Timestamp EARLIEST_ION_TIMESTAMP =
        Timestamp.valueOf("0001-01-01");

    private static final Timestamp UNIX_EPOCH_TIMESTAMP =
        Timestamp.valueOf("1970-01-01T00:00:00.000Z");

    /**
     * PST = -08:00 = -480
     */
    private static final int PST_OFFSET = -8 * 60;


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


    @Override
    public void badValue(String text)
    {
        super.badValue(text);

        try {
            Timestamp.valueOf(text);
            fail("Expected exception parsing text: " + text);
        }
        catch (IllegalArgumentException e) { }
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

        Timestamp ts = actual.timestampValue();
        Calendar tsCal = ts.calendarValue();
        assertEquals(0, expected.compareTo(tsCal));
        assertEquals("millis", expected.getTimeInMillis(), tsCal.getTimeInMillis());
        assertEquals("offset", expected.get(Calendar.ZONE_OFFSET),
                     tsCal.get(Calendar.ZONE_OFFSET));
    }


    private void checkFields(int expectedYear, int expectedMonth, int expectedDay,
                             int expectedHour, int expectedMinute,
                             int expectedSecond,
                             BigDecimal expectedFraction,
                             Integer expectedOffset,
                             Precision expectedPrecision,
                             Timestamp ts)
    {
        checkFields(expectedYear, expectedMonth, expectedDay,
                    expectedHour, expectedMinute, expectedSecond,
                    expectedFraction,
                    expectedOffset,
                    ts);
        assertEquals(expectedPrecision, ts.getPrecision());
    }

    private void checkFields(int expectedYear, int expectedMonth, int expectedDay,
                             int expectedHour, int expectedMinute,
                             int expectedSecond,
                             BigDecimal expectedFraction,
                             Integer expectedOffset,
                             Timestamp ts)
    {
        assertEquals("year",     expectedYear,     ts.getYear());
        assertEquals("month",    expectedMonth,    ts.getMonth());
        assertEquals("day",      expectedDay,      ts.getDay());
        assertEquals("hour",     expectedHour,     ts.getHour());
        assertEquals("minute",   expectedMinute,   ts.getMinute());
        assertEquals("second",   expectedSecond,   ts.getSecond());
        assertEquals("fraction", expectedFraction, ts.getFractionalSecond());
        assertEquals("offset",   expectedOffset,   ts.getLocalOffset());
    }

    private void checkTime(int expectedYear, int expectedMonth, int expectedDay,
                           String expectedText, String ionText)
    {
        Timestamp ts = new Timestamp(expectedYear, expectedMonth, expectedDay);
        checkFields(expectedYear, expectedMonth, expectedDay, 0, 0, 0, null,
                    UNKNOWN_OFFSET, DAY, ts);
        assertEquals(expectedText, ts.toString());

        checkTime(expectedYear, expectedMonth, expectedDay,
                  0, 0, 0, null,
                  UNKNOWN_OFFSET, expectedText, ionText);
    }

    private void checkTime(int expectedYear, int expectedMonth, int expectedDay,
                           String ionText)
    {
        String expectedText = ionText;
        checkTime(expectedYear, expectedMonth, expectedDay, expectedText,
                  ionText);
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
        Timestamp ts2 = Timestamp.valueOf(ionText.trim());

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

    /**
     * Perform sanity check on static constant Timestamps fields, as they are
     * used in other test methods.
     * <p>
     * <b>NOTE</b>: If this test method fails, you must re-run the entire test
     * class as other test methods might be based off the correctness of
     * assumptions declared here.
     */
    @Test
    public void testTimestampConstants()
    {
        checkFields(1, 1, 1, 0, 0, 0, null, null, DAY, EARLIEST_ION_TIMESTAMP);
        assertEquals("0001-01-01Z", EARLIEST_ION_TIMESTAMP.toZString());
        assertEquals("0001-01-01", EARLIEST_ION_TIMESTAMP.toString());
        assertEquals(-62135769600000L, EARLIEST_ION_TIMESTAMP.getMillis());

        checkFields(1970, 1, 1, 0, 0, 0, new BigDecimal("0.000"), 0, FRACTION, UNIX_EPOCH_TIMESTAMP);
        assertEquals("1970-01-01T00:00:00.000Z", UNIX_EPOCH_TIMESTAMP.toZString());
        assertEquals("1970-01-01T00:00:00.000Z", UNIX_EPOCH_TIMESTAMP.toString());
        assertEquals(0L, UNIX_EPOCH_TIMESTAMP.getMillis());
    }

    @Test
    public void testFactoryNullTimestamp()
    {
        IonTimestamp value = system().newNullTimestamp();
        checkNullTimestamp(value);
        modifyTimestamp(value);
    }

    @Test
    public void testTextNullTimestamp()
    {
        IonTimestamp value = (IonTimestamp) oneValue("null.timestamp");
        checkNullTimestamp(value);
        modifyTimestamp(value);
    }


    @Test
    public void testBadYears()
    {
        badValue("1969-");
        badValue("1969t");

        // No timezone allowed
        badValue("2000TZ");
        badValue("2000T-00:00");
    }

    @Test
    public void testBadMonths()
    {
        badValue("1696-02");
        badValue("1969-02-");
        badValue("1969-02t");

        // No timezone allowed
        badValue("2000-01TZ");
        badValue("2000-01T-00:00");
    }

    @Test
    public void testBadDates()
    {
        badValue("1696-02");
        badValue("1969-02-");
        badValue("1969-02t");
        badValue("1969-02-23t");
        badValue("1969-02-23t00:00Z");      // bad separator
        badValue("1969-02-23T00:00z");      // bad TZD

        badValue("1969-1x-23");

        // No timezone allowed
        badValue("2000-01-01TZ");
        badValue("2000-01-01T+00:00");
    }

    @Test
    public void testDateWithSlashes()
    {
        badValue("2007/02/21");
    }


    @Test
    public void testMillis()
    {
        checkTime(2001, 1, 1, 12, 34, 56, new BigDecimal(".789"),
                  UTC_OFFSET,
                  "2001-01-01T12:34:56.789Z");
    }

    @Test
    public void testBareDate()
    {
        checkTime(1969, 02, 23, "1969-02-23");
        checkTime(1969, 02, 23, "1969-02-23", "1969-02-23T");

        // Check following character
        checkTime(1969, 02, 23, "1969-02-23", "1969-02-23 ");
        checkTime(1969, 02, 23, "1969-02-23", "1969-02-23T ");
    }

    @Test
    public void testDateWithMinutes()
    {
        checkTime(1969, 02, 23, 0, 0, 0, null, UTC_OFFSET,
                  "1969-02-23T00:00Z");
    }

    @Test
    public void testDateWithSeconds()
    {
        checkTime(1969, 02, 23, 0, 0, 0, null, UTC_OFFSET,
                  "1969-02-23T00:00:00Z");
    }

    @Test
    public void testDateWithMillis()
    {
        checkTime(1969, 02, 23, 0, 0, 0, new BigDecimal("0.000"), UTC_OFFSET,
                  "1969-02-23T00:00:00.000Z");
    }

    @Test
    public void testDateWithMinutesAndPosTzd()
    {
        checkTime(1969, 02, 23, 0, 0, 0, null, UTC_OFFSET,
                  "1969-02-23T00:00Z",
                  "1969-02-23T00:00+00:00");
    }

    @Test
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

    @Test
    public void testPrecision()
    {
        checkCanonicalText("2007T");
        checkCanonicalText("2007-08T");
        checkCanonicalText("2007-08-28");
        checkCanonicalText("2007-08-28T16:37Z");
        checkCanonicalText("2007-08-28T16:37:24Z");
        checkCanonicalText("2007-08-28T16:37:24.0Z");
        checkCanonicalText("2007-08-28T16:37:24.00Z");
        checkCanonicalText("2007-08-28T16:37:24.000Z");
        checkCanonicalText("2007-08-28T16:37:24.0000Z");


        checkTime(1969, 01, 01, 0, 0, 0, null, null, "1969T");
        checkTime(1969, 02, 01, 0, 0, 0, null, null, "1969-02T");
        // we only get to pick 1 default output for a given precision: checkTime(1969, 02, 03, 0, 0, 0, null, null, "1969-02-03T");
        checkTime(1969, 02, 03, 0, 0, 0, null, null, "1969-02-03");
    }


    @Test
    public void testDateWithNormalTzd()
    {
        checkTime(1969, 02, 22, 16, 0, 0, null, new Integer(-480),
                  "1969-02-22T16:00-08:00");
    }

    @Test
    public void testdateWithOddTzd()
    {
        checkTime(1969, 02, 23, 1, 15, 0, new BigDecimal("0.00"), new Integer(75),
                  "1969-02-23T01:15:00.00+01:15");
    }

    @Test
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

    @Test
    public void testUnknownLocalOffset()
    {
        IonTimestamp value = (IonTimestamp) oneValue("2007-05-08T05:17-00:00");
        assertEquals(null, value.getLocalOffset());

        checkTime(2007, 5, 8, 5, 17, 0, null, Timestamp.UNKNOWN_OFFSET,
                  "2007-05-08T05:17-00:00");
    }

    @Test
    public void testDateWithZ()
    {
        badValue("1696-02-23Z");
    }

    @Test
    public void testTruncatedOffset()
    {
        badValue("2004-12-11T12:10:11+8");
    }

    @Test
    public void testTruncatedSeconds()
    {
        badValue("2004-12-11T12:10:1Z");
        badValue("2004-12-11T12:10:1-08:00");
        badValue("2004-12-11T12:10:1+08:00");
    }

    @Test
    public void testYearZero()
    {
        badValue("0000-01-01");
        badValue("0000-12-31");
    }

    @Test
    public void testNegativeYear()
    {
        badValue("[ -2000-01-01 ]");
    }

    @Test
    public void testPositiveYear()
    {
        badValue("[ +2000-01-01 ]");
    }

    @Test
    public void testYearOne()
    {
        checkTime(1, 1, 1, 0, 0, 0, null, Timestamp.UNKNOWN_OFFSET,
                  "0001-01-01");
        checkTime(1, 12, 31, 0, 0, 0, null, Timestamp.UNKNOWN_OFFSET,
                  "0001-12-31");
    }


    @Test
    public void testEquivalence()
    {
        IonTimestamp t1 = (IonTimestamp) oneValue("2009-04-25T16:07:00Z");
        IonTimestamp t2 = (IonTimestamp) oneValue("2009-04-25T15:06:00-01:01");
        IonTimestamp t3 = (IonTimestamp) oneValue("2009-04-25T16:07:00.000Z");
        IonTimestamp t4 = (IonTimestamp) oneValue("2009-04-25T16:07:00.0000Z");

        assertEquals(t1, t1);
        assertEquals(t1, t1.clone());

        notEqualButSameTime(t1, t2);
        notEqualButSameTime(t1, t3);
        notEqualButSameTime(t2, t4);
        notEqualButSameTime(t3, t4);
    }

    public void notEqualButSameTime(IonTimestamp t1, IonTimestamp t2)
    {
        assertFalse(t1.equals(t2));
        assertFalse(t2.equals(t1));
        assertEquals(0, t1.timestampValue().compareTo(t2.timestampValue()));
        assertEquals(0, t1.timestampValue().compareTo(t2.timestampValue()));
    }

    @Test
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


    @Test
    public void testTimestampClone()
        throws Exception
    {
        testSimpleClone("null.timestamp");
        testSimpleClone("2008T");
        testSimpleClone("2008-07T");
        testSimpleClone("2008-07-11");
        testSimpleClone("2008-07-11T14:49-12:34");
        testSimpleClone("2008-07-11T14:49:26+08:00");
        testSimpleClone("2008-07-11T14:49:26.00-07:00");
        testSimpleClone("2008-07-11T14:49:26.000-07:00");
        testSimpleClone("2008-07-11T14:49:26.01000-07:00");
    }

    @Test
    public void testMonthBoundaries()
    {
        // This traps a bug in leap-year calculation
        parse(   "2007-02-28T00:00Z"); // not leap year
        badValue("2007-02-29T00:00Z");

        parse(   "2008-02-29T00:00Z"); // leap year
        badValue("2008-02-30T00:00Z");

        parse(   "2009-01-31T00:00Z");
        badValue("2009-01-32T00:00Z");
        parse(   "2009-02-28T00:00Z");
        badValue("2009-02-29T00:00Z");
        parse(   "2009-03-31T00:00Z");
        badValue("2009-03-32T00:00Z");
        parse(   "2009-04-30T00:00Z");
        badValue("2009-04-31T00:00Z");
        parse(   "2009-05-31T00:00Z");
        badValue("2009-05-32T00:00Z");
        parse(   "2009-06-30T00:00Z");
        badValue("2009-06-31T00:00Z");
        parse(   "2009-07-31T00:00Z");
        badValue("2009-07-32T00:00Z");
        parse(   "2009-08-31T00:00Z");
        badValue("2009-08-32T00:00Z");
        parse(   "2009-09-30T00:00Z");
        badValue("2009-09-31T00:00Z");
        parse(   "2009-10-01T00:00+01:00"); // Trap for ION-71
        parse(   "2009-10-31T00:00Z");
        badValue("2009-10-32T00:00Z");
        parse(   "2009-11-30T00:00Z");
        badValue("2009-11-31T00:00Z");
        parse(   "2009-12-31T00:00Z");
        badValue("2009-12-32T00:00Z");
    }

    @Test
    public void testNewTimestamp()
    {
        // ===== Test on Timestamp(...) date precise constructor =====
        Timestamp ts = new Timestamp(2010, 2, 1);
        checkFields(2010, 2, 1, 0, 0, 0, null, UNKNOWN_OFFSET, DAY, ts);
        assertEquals("2010-02-01", ts.toString());
        assertEquals("2010-02-01Z", ts.toZString());

        // ===== Test on Timestamp(...) minute precise constructor =====
        ts = new Timestamp(2010, 2, 1, 10, 11, PST_OFFSET);
        checkFields(2010, 2, 1, 10, 11, 0, null, PST_OFFSET, MINUTE, ts);
        assertEquals("2010-02-01T10:11-08:00", ts.toString());
        assertEquals("2010-02-01T18:11Z", ts.toZString());

        ts = new Timestamp(2010, 2, 1, 10, 11, null);
        checkFields(2010, 2, 1, 10, 11, 0, null, null, MINUTE, ts);
        assertEquals("2010-02-01T10:11-00:00", ts.toString());
        assertEquals("2010-02-01T10:11Z", ts.toZString());

        // ===== Test on Timestamp(...) second precise constructor =====
        ts = new Timestamp(2010, 2, 1, 10, 11, 12, PST_OFFSET);
        checkFields(2010, 2, 1, 10, 11, 12, null, PST_OFFSET, SECOND, ts);
        assertEquals("2010-02-01T10:11:12-08:00", ts.toString());
        assertEquals("2010-02-01T18:11:12Z", ts.toZString());

        ts = new Timestamp(2010, 2, 1, 10, 11, 12, null);
        checkFields(2010, 2, 1, 10, 11, 12, null, null, SECOND, ts);
        assertEquals("2010-02-01T10:11:12-00:00", ts.toString());
        assertEquals("2010-02-01T10:11:12Z", ts.toZString());

        // ===== Test on Timestamp(...) fractional second precise constructor =====
        BigDecimal fraction = new BigDecimal(".34");
        ts = new Timestamp(2010, 2, 1, 10, 11, 12, fraction, PST_OFFSET);
        checkFields(2010, 2, 1, 10, 11, 12, fraction, PST_OFFSET, FRACTION, ts);
        assertEquals("2010-02-01T10:11:12.34-08:00", ts.toString());
        assertEquals("2010-02-01T18:11:12.34Z", ts.toZString());

        ts = new Timestamp(2010, 2, 1, 10, 11, 12, fraction, null);
        checkFields(2010, 2, 1, 10, 11, 12, fraction, null, FRACTION, ts);
        assertEquals("2010-02-01T10:11:12.34-00:00", ts.toString());
        assertEquals("2010-02-01T10:11:12.34Z", ts.toZString());

        // This was broken prior to R17. It had FRACTION precision, but no
        // fractional data, and didn't equal the same value created other ways.
        ts = new Timestamp(2010, 2, 1, 10, 11, 12, BigDecimal.ZERO, 0);
        assertEquals(Timestamp.valueOf("2010-02-01T10:11:12Z"), ts);
        checkFields(2010, 2, 1, 10, 11, 12, null, 0, SECOND, ts);

        // New static method unifies decimal seconds
        BigDecimal second = new BigDecimal("12.34");
        ts = Timestamp.forSecond(2010, 2, 1, 10, 11, second, PST_OFFSET);
        checkFields(2010, 2, 1, 10, 11, 12, fraction, PST_OFFSET, FRACTION, ts);
        assertEquals("2010-02-01T10:11:12.34-08:00", ts.toString());
        assertEquals("2010-02-01T18:11:12.34Z", ts.toZString());

        ts = Timestamp.forSecond(2010, 2, 1, 10, 11, second, null);
        checkFields(2010, 2, 1, 10, 11, 12, fraction, null, FRACTION, ts);
        assertEquals("2010-02-01T10:11:12.34-00:00", ts.toString());
        assertEquals("2010-02-01T10:11:12.34Z", ts.toZString());

        second = new BigDecimal("12.");
        ts = Timestamp.forSecond(2010, 2, 1, 10, 11, second, PST_OFFSET);
        checkFields(2010, 2, 1, 10, 11, 12, null, PST_OFFSET, SECOND, ts);
        assertEquals("2010-02-01T10:11:12-08:00", ts.toString());
        assertEquals("2010-02-01T18:11:12Z", ts.toZString());
    }

    /** Test for {@link Timestamp#Timestamp(BigDecimal, Integer)} */
    @Test
    public void testNewTimestampFromBigDecimal()
    {
        BigDecimal bigDec = new BigDecimal("1325419950555.123");

        Timestamp ts = new Timestamp(bigDec, PST_OFFSET);
        checkFields(2012, 1, 1, 4, 12, 30, new BigDecimal("0.555123"), PST_OFFSET, FRACTION, ts);
        assertEquals("2012-01-01T04:12:30.555123-08:00", ts.toString());
        assertEquals("2012-01-01T12:12:30.555123Z", ts.toZString());

        ts = new Timestamp(bigDec, null);
        checkFields(2012, 1, 1, 12, 12, 30, new BigDecimal("0.555123"), null, FRACTION, ts);
        assertEquals("2012-01-01T12:12:30.555123-00:00", ts.toString());
        assertEquals("2012-01-01T12:12:30.555123Z", ts.toZString());
    }


    @SuppressWarnings("unused")
    @Test (expected = NullPointerException.class)
    public void testNewTimestampFromBigDecimalWithNull()
    {
        Timestamp ts = new Timestamp(null, PST_OFFSET);
    }

    /**
     * Test for {@link Timestamp#Timestamp(BigDecimal, Integer)},
     * ensuring that varying BigDecimals with different scales produce
     * Timestamps (with correct precision of second/fractional seconds) as
     * expected.
     */
    @Test
    public void testNewTimestampFromBigDecimalWithDifferentScales()
    {
        // We're checking on the boundary: scale of -3
        BigDecimal decScaleNegFour      = new BigDecimal("132541995e4");
        BigDecimal decScaleNegThree     = new BigDecimal("1325419950e3"); // boundary
        BigDecimal decScaleNegTwo       = new BigDecimal("13254199505e2");
        BigDecimal decScaleNegOne       = new BigDecimal("132541995055e1");
        BigDecimal decScaleZero         = new BigDecimal("1325419950555");
        BigDecimal decScalePosOne       = new BigDecimal("1325419950555.5");

        // Sanity check to ensure that the varying BigDecimal parameters have
        // the correct scales we're testing.
        assertEquals(-4, decScaleNegFour.scale());
        assertEquals(-3, decScaleNegThree.scale());
        assertEquals(-2, decScaleNegTwo.scale());
        assertEquals(-1, decScaleNegOne.scale());
        assertEquals( 0, decScaleZero.scale());
        assertEquals( 1, decScalePosOne.scale());

        Timestamp ts;

        ts = new Timestamp(decScaleNegFour, null);
        checkFields(2012, 1, 1, 12, 12, 30, null, null, SECOND, ts);

        ts = new Timestamp(decScaleNegThree, null);
        checkFields(2012, 1, 1, 12, 12, 30, new BigDecimal("0"), null, FRACTION, ts);

        ts = new Timestamp(decScaleNegTwo, null);
        checkFields(2012, 1, 1, 12, 12, 30, new BigDecimal("0.5"), null, FRACTION, ts);

        ts = new Timestamp(decScaleNegOne, null);
        checkFields(2012, 1, 1, 12, 12, 30, new BigDecimal("0.55"), null, FRACTION, ts);

        ts = new Timestamp(decScaleZero, null);
        checkFields(2012, 1, 1, 12, 12, 30, new BigDecimal("0.555"), null, FRACTION, ts);

        ts = new Timestamp(decScalePosOne, null);
        checkFields(2012, 1, 1, 12, 12, 30, new BigDecimal("0.5555"), null, FRACTION, ts);
    }

    /** Test for {@link Timestamp#Timestamp(long, Integer)} */
    @Test
    public void testNewTimestampFromLong()
    {
        long actualMillis = 1265019072340L;

        Timestamp ts = new Timestamp(actualMillis, PST_OFFSET);
        checkFields(2010, 2, 1, 2, 11, 12, new BigDecimal("0.340"), PST_OFFSET, FRACTION, ts);
        assertEquals("2010-02-01T02:11:12.340-08:00", ts.toString());
        assertEquals("2010-02-01T10:11:12.340Z", ts.toZString());

        ts = new Timestamp(actualMillis, null);
        checkFields(2010, 2, 1, 10, 11, 12, new BigDecimal("0.340"), null, FRACTION, ts);
        assertEquals("2010-02-01T10:11:12.340-00:00", ts.toString());
        assertEquals("2010-02-01T10:11:12.340Z", ts.toZString());
    }

    /**
     * Test for {@link Timestamp#createFromUtcFields(Precision, int, int, int, int, int, int, BigDecimal, Integer)}
     * ensuring that varying precisions produce Timestamps as expected as per
     * precision "narrowing".
     */
    @Test
    public void testNewTimestampFromUtcFieldsWithDifferentPrecisions()
    {
        // Non-varying time components
        int zyear               = 2012;
        int zmonth              = 2;
        int zday                = 3;
        int zhour               = 4;
        int zminute             = 5;
        int zsecond             = 6;
        BigDecimal zfrac        = new BigDecimal("0.007");
        Integer offset          = null;

        // Varying precisions
        Precision p;
        Timestamp ts;

        p = YEAR;
        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset);
        // 2012T
        checkFields(zyear, 1, 1, 0, 0, 0, null, offset, p, ts);

        p = MONTH;
        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset);
        // 2012-02T
        checkFields(zyear, zmonth, 1, 0, 0, 0, null, offset, p, ts);

        p = DAY;
        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset);
        // 2012-02-03Z
        checkFields(zyear, zmonth, zday, 0, 0, 0, null, offset, p, ts);

        p = MINUTE;
        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset);
        // 2012-02-03T04:05-00:00
        checkFields(zyear, zmonth, zday, zhour, zminute, 0, null, offset, p, ts);

        p = SECOND;
        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset);
        // 2012-02-03T04:05:06-00:00
        checkFields(zyear, zmonth, zday, zhour, zminute, zsecond, null, offset, p, ts);

        p = FRACTION;
        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset);
        // 2012-02-03T04:05:06.007-00:00
        checkFields(zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset, p, ts);

        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, BigDecimal.ZERO, 0);
        assertEquals(Timestamp.valueOf("2012-02-03T04:05:06Z"), ts);
    }

    /**
     * Test for {@link Timestamp#createFromUtcFields(Precision, int, int, int, int, int, int, BigDecimal, Integer)}
     * ensuring that varying local offsets produce Timestamps as expected.
     */
    @Test
    public void testNewTimestampFromUtcFieldsWithDifferentOffsets()
    {
        // Non-varying time components
        int zyear               = 2012;
        int zmonth              = 2;
        int zday                = 3;
        int zhour               = 4;
        int zminute             = 5;
        int zsecond             = 6;
        BigDecimal zfrac        = new BigDecimal("0.007");
        Precision p             = FRACTION;

        // Varying local offsets (in minutes)
        Integer offset;
        Timestamp ts;

        offset = null;  // unknown local offset
        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset);
        // 2012-02-03T04:05:06.007-00:00
        checkFields(zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset, p, ts);

        offset = 0;     // zero local offset
        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset);
        // 2012-02-03T04:05:06.007Z
        checkFields(zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset, p, ts);

        offset = 480;   // 8 hours
        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset);
        // 2012-02-03T12:05:06.007+08:00
        checkFields(zyear, zmonth, zday, 12, zminute, zsecond, zfrac, offset, p, ts);

        offset = -480;  // -8 hours
        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset);
        // 2012-02-02T20:05:06.007-08:00
        checkFields(zyear, zmonth, 2, 20, zminute, zsecond, zfrac, offset, p, ts);

        offset = 123;   // 2 hours 3 minutes
        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset);
        // 2012-02-03T06:08:06.007+02:03
        checkFields(zyear, zmonth, zday, 6, 8, zsecond, zfrac, offset, p, ts);

        offset = -123;  // -2 hours 3 minutes
        ts = Timestamp.createFromUtcFields(p, zyear, zmonth, zday, zhour, zminute, zsecond, zfrac, offset);
        // 2012-02-03T02:02:06.007-02:03
        checkFields(zyear, zmonth, zday, 2, 2, zsecond, zfrac, offset, p, ts);
    }

    /**
     * Test for {@link Timestamp#Timestamp(Calendar)},
     * ensure that varying Calendar fields produces the expected Timestamps
     * with the correct precision, local offset, and time field values.
     */
    @Test
    public void testNewTimestampFromCalendar()
    {
        Calendar cal = makeUtcCalendar();

        // ===== Timestamp year precision =====
        cal.clear();
        cal.set(Calendar.YEAR, 2009);
        assertFalse(cal.isSet(Calendar.MONTH));

        Timestamp ts = new Timestamp(cal);
        checkFields(2009, 1, 1, 0, 0, 0, null, 0, YEAR, ts);
        assertEquals("2009T", ts.toString());
        assertEquals("2009T", ts.toZString());

        // ===== Timestamp month precision =====
        cal.clear();
        cal.set(Calendar.YEAR, 2009);
        cal.set(Calendar.MONTH, 2);
        assertFalse(cal.isSet(Calendar.HOUR_OF_DAY));

        ts = new Timestamp(cal);
        checkFields(2009, 3, 1, 0, 0, 0, null, 0, MONTH, ts);
        assertEquals("2009-03T", ts.toString());
        assertEquals("2009-03T", ts.toZString());

        // ===== Timestamp day precision =====
        cal.clear();
        cal.set(2009, 2, 18);
        assertFalse(cal.isSet(Calendar.HOUR_OF_DAY) &&
                    cal.isSet(Calendar.MINUTE));

        ts = new Timestamp(cal);
        checkFields(2009, 3, 18, 0, 0, 0, null, 0, DAY, ts);
        assertEquals("2009-03-18Z", ts.toString());
        assertEquals("2009-03-18Z", ts.toZString());

        // ===== Timestamp minute precision =====
        cal.clear();
        cal.set(2009, 1, 1, 10, 11);
        assertFalse(cal.isSet(Calendar.SECOND));

        ts = new Timestamp(cal);
        checkFields(2009, 2, 1, 10, 11, 0, null, 0, MINUTE, ts);
        assertEquals("2009-02-01T10:11Z", ts.toString());
        assertEquals("2009-02-01T10:11Z", ts.toZString());

        // ===== Timestamp second precision =====
        cal.clear();
        cal.set(2009, 1, 1, 10, 11, 12);
        assertFalse(cal.isSet(Calendar.MILLISECOND));

        ts = new Timestamp(cal);
        checkFields(2009, 2, 1, 10, 11, 12, null, 0, SECOND, ts);
        assertEquals("2009-02-01T10:11:12Z", ts.toString());
        assertEquals("2009-02-01T10:11:12Z", ts.toZString());

        // ===== Timestamp fractional second precision =====
        cal.clear();
        cal.set(2009, 1, 1, 10, 11, 12);
        cal.set(Calendar.MILLISECOND, 345);
        assertTrue(cal.isSet(Calendar.MILLISECOND));

        ts = new Timestamp(cal);
        checkFields(2009, 2, 1, 10, 11, 12, new BigDecimal("0.345"), 0, FRACTION, ts);
        assertEquals("2009-02-01T10:11:12.345Z", ts.toString());
        assertEquals("2009-02-01T10:11:12.345Z", ts.toZString());

        // ===== Timestamp local offset =====
        cal.clear();
        cal.set(2009, 1, 1, 10, 11, 12);
        cal.set(Calendar.MILLISECOND, 345);
        cal.setTimeZone(TimeZone.getTimeZone("PST"));
        assertEquals(TimeZone.getTimeZone("PST"), cal.getTimeZone());

        ts = new Timestamp(cal);
        checkFields(2009, 2, 1, 10, 11, 12, new BigDecimal("0.345"), PST_OFFSET, FRACTION, ts);
        assertEquals("2009-02-01T10:11:12.345-08:00", ts.toString());
        assertEquals("2009-02-01T18:11:12.345Z", ts.toZString());
    }


    /** Trap for ION-280 */
    @Test
    public void testFromCalendarWithDaylightSavingsTime()
    {
        Calendar cal = makeUtcCalendar();
        cal.clear();
        cal.set(2012, 2, 9, 10, 11, 12);
        cal.set(Calendar.MILLISECOND, 345);
        cal.setTimeZone(TimeZone.getTimeZone("PST"));
        Timestamp ts = new Timestamp(cal);
        checkFields(2012, 3, 9, 10, 11, 12, new BigDecimal("0.345"), PST_OFFSET, FRACTION, ts);
        assertEquals("2012-03-09T10:11:12.345-08:00", ts.toString());

        // Move forward into daylight savings
        cal.add(Calendar.DAY_OF_MONTH, 5);
        ts = new Timestamp(cal);
        checkFields(2012, 3, 14, 10, 11, 12, new BigDecimal("0.345"), -420, FRACTION, ts);
        assertEquals("2012-03-14T10:11:12.345-07:00", ts.toString());
    }


    @Test(expected = IllegalArgumentException.class)
    public void testNewTimestampFromClearCalendar()
    {
        Calendar cal = makeUtcCalendar();
        cal.clear();
        new Timestamp(cal);
    }


    @Test
    public void testTimestampWithNegativeFraction()
    {
        BigDecimal frac = Decimal.negativeZero(3);

        Timestamp ts = new Timestamp(2000, 11, 14, 17, 30, 12, frac, 0);
        assertEquals("2000-11-14T17:30:12.000Z", ts.toString());

        frac = new BigDecimal("-0.123");
        ts = new Timestamp(2000, 11, 14, 17, 30, 12, frac, 0);
        assertEquals("2000-11-14T17:30:12.123Z", ts.toString());
    }

    @Test
    public void testNewTimestampWithNullFraction()
    {
        BigDecimal frac = null;
        try {
            new Timestamp(2000, 11, 14, 17, 30, 12, frac, 0);
            fail("Expected exception");
        }
        catch (NullPointerException e) { }

        try {
            Timestamp.createFromUtcFields(FRACTION,
                                          2000, 11, 14, 17, 30, 12, frac, 0);
            fail("Expected exception");
        }
        catch (NullPointerException e) { }
    }


    @Test
    public void testTimestampForSecondValidation()
    {
        try {
            Timestamp.forSecond(2000, 11, 14, 17, 30, null, 0);
            fail("Expected exception");
        }
        catch (NullPointerException e) { }

        try {
            Timestamp.forSecond(2000, 11, 14, 17, 30, new BigDecimal("-1"), 0);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e) { }

        try {
            Timestamp.forSecond(2000, 11, 14, 17, 30, new BigDecimal("60"), 0);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e) { }
    }

    @Test
    public void testTimestampForSecondNegativeZero()
    {
        Timestamp a =
            Timestamp.forSecond(2000, 11, 14, 17, 30, 0, 0);
        Timestamp b =
            Timestamp.forSecond(2000, 11, 14, 17, 30, NEGATIVE_ZERO, 0);
        assertEquals(a, b);

        a = Timestamp.forSecond(2000, 11, 14, 17, 30, new BigDecimal("0.000"), 0);
        b = Timestamp.forSecond(2000, 11, 14, 17, 30, negativeZero(3),         0);
        assertEquals(a, b);
    }

    @Test
    public void testValueOfNullTimestamp()
    {
        Timestamp nt = Timestamp.valueOf("null.timestamp");
        assertEquals(null, nt);
    }

    @Test
    public void testValueOfBadNullTimestamp()
    {
        try {
            Timestamp.valueOf("null.timestam");
            fail("Expected exception");
        }
        catch (IllegalArgumentException e) { }

        try {
            Timestamp.valueOf("null.timestamps");
            fail("Expected exception");
        }
        catch (IllegalArgumentException e) { }
    }

    /**
     * Trap for ION-124
     */
    @Test
    public void testValueOfWithNonsenseText()
    {
        try {
            Timestamp.valueOf("nomnomnom");      // shorter than null.timestamp
            fail("Expected exception");
        }
        catch (IllegalArgumentException e) { }

        try {
            Timestamp.valueOf("nomnomnomnomnom"); // longer than null.timestamp
            fail("Expected exception");
        }
        catch (IllegalArgumentException e) { }
    }

    /**
     * TODO This replicates a file in the test data suite.
     * We should really be scanning all relevant data files.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testValueOfWithMissingFraction()
    {
        Timestamp.valueOf("2010-11-17T12:34:56.Z");
    }

    /** Trap for ION-193 */
    @Test
    public void testLocalBoundaryPositive() {
        // UTC is last month
        final Timestamp ts = Timestamp.valueOf("2010-07-01T02:00:41+02:15");
        assertEquals(7, ts.getMonth());
        assertEquals(6, ts.getZMonth());
        assertEquals(1, ts.getDay());
        assertEquals(30, ts.getZDay());
        assertEquals(2, ts.getHour());
        assertEquals(23, ts.getZHour());
        assertEquals(0, ts.getMinute());
        assertEquals(45, ts.getZMinute());
    }

    /** Trap for ION-193 */
    @Test
    public void testLocalBoundaryNegative() {
        // UTC is next month
        final Timestamp ts = Timestamp.valueOf("2010-02-28T22:00:41-07:00");
        assertEquals(2, ts.getMonth());
        assertEquals(3, ts.getZMonth());
        assertEquals(28, ts.getDay());
        assertEquals(1, ts.getZDay());
        assertEquals(22, ts.getHour());
        assertEquals(5, ts.getZHour());
        assertEquals(0, ts.getMinute());
        assertEquals(0, ts.getZMinute());
    }

    @Test
    public void testForDateZ()
    {
        Date now = new Date();
        Timestamp ts = Timestamp.forDateZ(now);
        assertEquals(now.getTime(), ts.getMillis());
        assertEquals(Timestamp.UTC_OFFSET, ts.getLocalOffset());

        now.setTime(0);
        ts = Timestamp.forDateZ(now);
        assertEquals(0, ts.getMillis());
        assertEquals(Timestamp.UTC_OFFSET, ts.getLocalOffset());
        assertEquals(1970, ts.getYear());
    }

    @Test
    public void testForDateZNull()
    {
        assertEquals(null, Timestamp.forDateZ(null));
    }


    @Test
    public void testForSqlTimestampZ()
    {
        long millis = System.currentTimeMillis();
        java.sql.Timestamp now = new java.sql.Timestamp(millis);
        assertEquals(millis % 1000 * 1000000, now.getNanos());

        Timestamp ts = Timestamp.forSqlTimestampZ(now);
        assertEquals(now.getTime(), ts.getMillis());
        assertEquals(Timestamp.UTC_OFFSET, ts.getLocalOffset());

        BigDecimal frac = ts.getFractionalSecond();
        frac = frac.movePointRight(9); // Convert to nanos
        assertEquals("nanos", now.getNanos(), frac.intValue());

        now.setTime(0);
        ts = Timestamp.forSqlTimestampZ(now);
        assertEquals(0, ts.getMillis());
        assertEquals(Timestamp.UTC_OFFSET, ts.getLocalOffset());
        assertEquals(1970, ts.getYear());
    }

    @Test
    public void testForSqlTimestampZNull()
    {
        assertEquals(null, Timestamp.forSqlTimestampZ(null));
    }

    @Test
    public void testLocalYearBoundaryPositive() {
        // UTC is previous year
        final Timestamp ts = Timestamp.valueOf("2007-01-01T00:00:01+09:00");
        assertEquals(2007, ts.getYear());
        assertEquals(2006, ts.getZYear());
        assertEquals(01, ts.getMonth());
        assertEquals(12, ts.getZMonth());
        assertEquals(01, ts.getDay());
        assertEquals(31, ts.getZDay());
        assertEquals(0, ts.getHour());
        assertEquals(15, ts.getZHour());
        assertEquals(00, ts.getMinute());
        assertEquals(00, ts.getZMinute());
    }

    @Test
    public void testLocalYearBoundaryNegative() {
        // UTC is next year
        final Timestamp ts = Timestamp.valueOf("2006-12-31T23:59:59-08:00");
        assertEquals(2006, ts.getYear());
        assertEquals(2007, ts.getZYear());
        assertEquals(12, ts.getMonth());
        assertEquals(1, ts.getZMonth());
        assertEquals(31, ts.getDay());
        assertEquals(1, ts.getZDay());
        assertEquals(23, ts.getHour());
        assertEquals(7, ts.getZHour());
        assertEquals(59, ts.getMinute());
        assertEquals(59, ts.getZMinute());
    }

    @Test
    public void testTimestampCopyConstructor()
    {
        BigDecimal second = new BigDecimal("12.3456789");
        Timestamp expectedTs =
            Timestamp.forSecond(2010, 2, 1, 10, 11, second, PST_OFFSET);
        assertEquals("2010-02-01T10:11:12.3456789-08:00",
                     expectedTs.toString());

        //===== Test on Timestamp(...) fractional precision copy-constructor =====
        Timestamp actualTs = new Timestamp(expectedTs.getYear(),
                                           expectedTs.getMonth(),
                                           expectedTs.getDay(),
                                           expectedTs.getHour(),
                                           expectedTs.getMinute(),
                                           expectedTs.getSecond(),
                                           expectedTs.getFractionalSecond(),
                                           expectedTs.getLocalOffset());
        assertEquals(expectedTs, actualTs);

        //===== Test on Timestamp.createFromUtcFields(...) copy-constructor =====
        actualTs = Timestamp
            .createFromUtcFields(expectedTs.getPrecision(),
                                 expectedTs.getZYear(),
                                 expectedTs.getZMonth(),
                                 expectedTs.getZDay(),
                                 expectedTs.getZHour(),
                                 expectedTs.getZMinute(),
                                 expectedTs.getZSecond(),
                                 expectedTs.getZFractionalSecond(),
                                 expectedTs.getLocalOffset());
        assertEquals(expectedTs, actualTs);
    }

    /**
     * Trap for ION-324 - Ensures that a point in time represented by a
     * millisecond value that is an offset from the Epoch with
     * fractional precision has its fractional seconds computed correctly during
     * construction of the Timestamp.
     * <p>
     * All variants of constructors that constructs a Timestamp with from a
     * milliseconds representation with fractional precision is tested here.
     * All constructors are expected to construct a valid Timestamp instance.
     */
    @Test
    public void testNegativeEpochWithFractionalSeconds()
    {
        // an instance of negative UNIX epoch time in milliseconds, the
        // UNIX epoch time is 0 milliseconds (i.e. 1970-01-01T00:00:00Z)
        BigDecimal negativeEpochDecimalMillis =
            new BigDecimal("-9223372036854.775808");

        Timestamp expectedTs =
            new Timestamp(negativeEpochDecimalMillis, UTC_OFFSET);
        long expectedMillis = expectedTs.getMillis();
        Timestamp expectedTsMillis =
            new Timestamp(expectedMillis, UTC_OFFSET);
        Timestamp expectedTsMillisWithNanosPrecision =
            Timestamp.valueOf("1677-09-21T00:12:43.145000000Z");

        // sanity check to ensure we indeed have a negative epoch point in time
        // with proper fractional seconds
        assertEquals("-9223372036854.775808",
                     negativeEpochDecimalMillis.toPlainString());

        assertEquals(-9223372036855L, expectedMillis);

        assertEquals("1677-09-21T00:12:43.145224192Z",
                     expectedTs.toZString());

        assertEquals("1677-09-21T00:12:43.145000000Z",
                     expectedTsMillisWithNanosPrecision.toZString());

        assertEquals("1677-09-21T00:12:43.145Z",
                     expectedTsMillis.toZString());

        // expect: expectedTs is after earliest Ion timestamp
        assertTrue(expectedTs.compareTo(EARLIEST_ION_TIMESTAMP) > 0);

        // expect: expectedTs is before unix epoch timestamp
        assertTrue(expectedTs.compareTo(UNIX_EPOCH_TIMESTAMP) < 0);



        //===== Test on Timestamp(BigDecimal, Integer) constructor =====
        Timestamp actualTs = new Timestamp(negativeEpochDecimalMillis, UTC_OFFSET);
        assertEquals(expectedTs, actualTs);


        //===== Test on Timestamp(long, Integer) constructor =====
        actualTs = new Timestamp(expectedMillis, UTC_OFFSET);
        assertEquals(expectedTsMillis, actualTs);


        //===== Test on Timestamp.forSqlTimestampZ() constructor =====
        java.sql.Timestamp actualSqlTs = new java.sql.Timestamp(expectedMillis);

        // milliseconds value, with nanoseconds precision
        actualTs = Timestamp.forSqlTimestampZ(actualSqlTs);
        assertEquals(expectedTsMillisWithNanosPrecision, actualTs);

        // milliseconds and nanoseconds value, with nanoseconds precision
        actualSqlTs.setNanos(145224192);
        actualTs = Timestamp.forSqlTimestampZ(actualSqlTs);
        assertEquals(expectedTs, actualTs);
    }

}
