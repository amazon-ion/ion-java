// Copyright (c) 2007-2011 Amazon.com, Inc.  All rights reserved.
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
     * The UTC TimeZone.
     *
     * TODO determine if this is well-defined.
     */
    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

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
                           String expectedText, String ionText)
    {
        Timestamp ts = new Timestamp(expectedYear, expectedMonth, expectedDay);
        checkFields(expectedYear, expectedMonth, expectedDay, 0, 0, 0, null,
                    UNKNOWN_OFFSET, ts);
        assertEquals(Precision.DAY, ts.getPrecision());
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
    public void testTimestampsFromSuite()
        throws Exception
    {
        Iterable<IonValue> values = loadTestFile("good/timestamps.ion");
        // File is a sequence of many timestamp values.

        int count = 0;

        for (IonValue value : values)
        {
            count++;
            String v = value.toString();
            assertTrue(value instanceof IonTimestamp || v.equals("just some crap that can't be"));
        }
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
        parse(   "2009-10-01T00:00+01:00"); // JIRA ION-71
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
        Timestamp ts = new Timestamp(2010, 2, 1, 10, 11, PST_OFFSET);
        checkFields(2010, 2, 1, 10, 11, 0, null, PST_OFFSET, ts);
        assertEquals("2010-02-01T10:11-08:00", ts.toString());
        assertEquals("2010-02-01T18:11Z", ts.toZString());

        ts = new Timestamp(2010, 2, 1, 10, 11, null);
        checkFields(2010, 2, 1, 10, 11, 0, null, null, ts);
        assertEquals("2010-02-01T10:11-00:00", ts.toString());
        assertEquals("2010-02-01T10:11Z", ts.toZString());


        ts = new Timestamp(2010, 2, 1, 10, 11, 12, PST_OFFSET);
        checkFields(2010, 2, 1, 10, 11, 12, null, PST_OFFSET, ts);
        assertEquals("2010-02-01T10:11:12-08:00", ts.toString());
        assertEquals("2010-02-01T18:11:12Z", ts.toZString());

        ts = new Timestamp(2010, 2, 1, 10, 11, 12, null);
        checkFields(2010, 2, 1, 10, 11, 12, null, null, ts);
        assertEquals("2010-02-01T10:11:12-00:00", ts.toString());
        assertEquals("2010-02-01T10:11:12Z", ts.toZString());


        BigDecimal fraction = new BigDecimal(".34");
        ts = new Timestamp(2010, 2, 1, 10, 11, 12, fraction, PST_OFFSET);
        checkFields(2010, 2, 1, 10, 11, 12, fraction, PST_OFFSET, ts);
        assertEquals("2010-02-01T10:11:12.34-08:00", ts.toString());
        assertEquals("2010-02-01T18:11:12.34Z", ts.toZString());

        ts = new Timestamp(2010, 2, 1, 10, 11, 12, fraction, null);
        checkFields(2010, 2, 1, 10, 11, 12, fraction, null, ts);
        assertEquals("2010-02-01T10:11:12.34-00:00", ts.toString());
        assertEquals("2010-02-01T10:11:12.34Z", ts.toZString());
    }


    @Test
    public void testNewTimestampFromCalendar()
    {
        Calendar cal = makeUtcCalendar();
        cal.clear();
        cal.set(Calendar.YEAR, 2009);
        assertFalse(cal.isSet(Calendar.MONTH));

        Timestamp ts = new Timestamp(cal);
        assertEquals(Timestamp.Precision.YEAR, ts.getPrecision());
        assertEquals(2009, ts.getYear());
        assertEquals(1, ts.getMonth());
        assertEquals(1, ts.getDay());
        assertEquals(0, ts.getLocalOffset().intValue());

        cal.clear();
        cal.set(Calendar.YEAR, 2009);
        cal.set(Calendar.MONTH, 2);
        assertFalse(cal.isSet(Calendar.HOUR_OF_DAY));

        ts = new Timestamp(cal);
        assertEquals(Timestamp.Precision.MONTH, ts.getPrecision());
        assertEquals(2009, ts.getYear());
        assertEquals(3, ts.getMonth());
        assertEquals(1, ts.getDay());
        assertEquals(0, ts.getLocalOffset().intValue());

        cal.clear();
        cal.set(2009, 1, 1, 10, 11, 12);
        cal.setTimeZone(TimeZone.getTimeZone("PST"));
        ts = new Timestamp(cal);
        assertEquals(Timestamp.Precision.SECOND, ts.getPrecision());
        assertEquals(2009, ts.getYear());
        assertEquals(2, ts.getMonth());
        assertEquals(1, ts.getDay());
        assertEquals(10, ts.getHour());
        assertEquals(11, ts.getMinute());
        assertEquals(12, ts.getSecond());
        assertEquals(PST_OFFSET, ts.getLocalOffset().intValue());


        cal = makeUtcCalendar(); // reset time zone
        cal.clear();
        cal.set(2009, 2, 18);
        assertFalse(cal.isSet(Calendar.HOUR_OF_DAY));

        ts = new Timestamp(cal);
        assertEquals(Timestamp.Precision.DAY, ts.getPrecision());
        assertEquals(2009, ts.getYear());
        assertEquals(3, ts.getMonth());
        assertEquals(18, ts.getDay());
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
            Timestamp.createFromUtcFields(Timestamp.Precision.FRACTION,
                                          2000, 11, 14, 17, 30, 12, frac, 0);
            fail("Expected exception");
        }
        catch (NullPointerException e) { }
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
}
