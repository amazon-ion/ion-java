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

import static software.amazon.ion.Decimal.NEGATIVE_ZERO;
import static software.amazon.ion.Decimal.negativeZero;
import static software.amazon.ion.Timestamp.UNKNOWN_OFFSET;
import static software.amazon.ion.Timestamp.UTC_OFFSET;
import static software.amazon.ion.Timestamp.createFromUtcFields;
import static software.amazon.ion.Timestamp.Precision.DAY;
import static software.amazon.ion.Timestamp.Precision.MINUTE;
import static software.amazon.ion.Timestamp.Precision.MONTH;
import static software.amazon.ion.Timestamp.Precision.SECOND;
import static software.amazon.ion.Timestamp.Precision.YEAR;
import static software.amazon.ion.impl.PrivateUtils.UTC;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.junit.Test;
import software.amazon.ion.Decimal;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.NullValueException;
import software.amazon.ion.Timestamp;
import software.amazon.ion.Timestamp.Precision;



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

        @SuppressWarnings("deprecation")
        Timestamp ts2 =
            createFromUtcFields(expectedPrecision,
                                ts.getZYear(), ts.getZMonth(), ts.getZDay(),
                                ts.getZHour(), ts.getZMinute(), ts.getZSecond(),
                                ts.getZFractionalSecond(), expectedOffset);
        assertEquals(ts2, ts);
    }

    private void checkFields(int expectedYear, int expectedMonth, int expectedDay,
                             int expectedHour, int expectedMinute,
                             int expectedSecond,
                             BigDecimal expectedFraction,
                             Integer expectedOffset,
                             Timestamp ts)
    {
        BigDecimal expectedDecimalSeconds = BigDecimal.valueOf(expectedSecond);
        if (expectedFraction != null) {
            expectedDecimalSeconds = expectedDecimalSeconds.add(expectedFraction);
        }

        assertEquals("year",     expectedYear,     ts.getYear());
        assertEquals("month",    expectedMonth,    ts.getMonth());
        assertEquals("day",      expectedDay,      ts.getDay());
        assertEquals("hour",     expectedHour,     ts.getHour());
        assertEquals("minute",   expectedMinute,   ts.getMinute());
        assertEquals("second",   expectedSecond,   ts.getSecond());
        assertEquals("fraction", expectedDecimalSeconds, ts.getDecimalSecond());
        assertEquals("offset",   expectedOffset,   ts.getLocalOffset());
    }

    private void checkTime(int expectedYear, int expectedMonth, int expectedDay,
                           String expectedText, String ionText)
    {
        Timestamp ts = Timestamp.forDay(expectedYear, expectedMonth, expectedDay);
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
        assertEquals("0001-01-01", EARLIEST_ION_TIMESTAMP.toZString());
        assertEquals("0001-01-01", EARLIEST_ION_TIMESTAMP.toString());
        assertEquals(-62135769600000L, EARLIEST_ION_TIMESTAMP.getMillis());

        checkFields(1970, 1, 1, 0, 0, 0, new BigDecimal("0.000"), 0, SECOND, UNIX_EPOCH_TIMESTAMP);
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
    public void testDateWithOddTzd()
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
        parse(   "2009-10-01T00:00+01:00");
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
        Timestamp ts = Timestamp.forDay(2010, 2, 1);
        checkFields(2010, 2, 1, 0, 0, 0, null, UNKNOWN_OFFSET, DAY, ts);
        assertEquals("2010-02-01", ts.toString());
        assertEquals("2010-02-01", ts.toZString());

        // ===== Test on Timestamp(...) minute precise constructor =====
        ts = Timestamp.forMinute(2010, 2, 1, 10, 11, PST_OFFSET);
        checkFields(2010, 2, 1, 10, 11, 0, null, PST_OFFSET, MINUTE, ts);
        assertEquals("2010-02-01T10:11-08:00", ts.toString());
        assertEquals("2010-02-01T18:11Z", ts.toZString());

        ts = Timestamp.forMinute(2010, 2, 1, 10, 11, null);
        checkFields(2010, 2, 1, 10, 11, 0, null, null, MINUTE, ts);
        assertEquals("2010-02-01T10:11-00:00", ts.toString());
        assertEquals("2010-02-01T10:11Z", ts.toZString());

        // ===== Test on Timestamp(...) second precise constructor =====
        ts = Timestamp.forSecond(2010, 2, 1, 10, 11, 12, PST_OFFSET);
        checkFields(2010, 2, 1, 10, 11, 12, null, PST_OFFSET, SECOND, ts);
        assertEquals("2010-02-01T10:11:12-08:00", ts.toString());
        assertEquals("2010-02-01T18:11:12Z", ts.toZString());

        ts = Timestamp.forSecond(2010, 2, 1, 10, 11, 12, null);
        checkFields(2010, 2, 1, 10, 11, 12, null, null, SECOND, ts);
        assertEquals("2010-02-01T10:11:12-00:00", ts.toString());
        assertEquals("2010-02-01T10:11:12Z", ts.toZString());

        BigDecimal frac = new BigDecimal("0.34");
        BigDecimal decimalSeconds = new BigDecimal("12.34");
        ts = Timestamp.forSecond(2010, 2, 1, 10, 11, decimalSeconds, PST_OFFSET);
        checkFields(2010, 2, 1, 10, 11, 12, frac, PST_OFFSET, SECOND, ts);
        assertEquals("2010-02-01T10:11:12.34-08:00", ts.toString());
        assertEquals("2010-02-01T18:11:12.34Z", ts.toZString());

        ts = Timestamp.forSecond(2010, 2, 1, 10, 11, decimalSeconds, null);
        checkFields(2010, 2, 1, 10, 11, 12, frac, null, SECOND, ts);
        assertEquals("2010-02-01T10:11:12.34-00:00", ts.toString());
        assertEquals("2010-02-01T10:11:12.34Z", ts.toZString());

        decimalSeconds = new BigDecimal("12.");
        ts = Timestamp.forSecond(2010, 2, 1, 10, 11, decimalSeconds, PST_OFFSET);
        checkFields(2010, 2, 1, 10, 11, 12, null, PST_OFFSET, SECOND, ts);
        assertEquals("2010-02-01T10:11:12-08:00", ts.toString());
        assertEquals("2010-02-01T18:11:12Z", ts.toZString());
    }

    /** Test for {@link Timestamp#forMillis(BigDecimal, Integer)} */
    @Test
    public void testNewTimestampFromBigDecimal()
    {
        BigDecimal bigDec = new BigDecimal("1325419950555.123");

        Timestamp ts = Timestamp.forMillis(bigDec, PST_OFFSET);
        checkFields(2012, 1, 1, 4, 12, 30, new BigDecimal("0.555123"), PST_OFFSET, SECOND, ts);
        assertEquals("2012-01-01T04:12:30.555123-08:00", ts.toString());
        assertEquals("2012-01-01T12:12:30.555123Z", ts.toZString());

        ts = Timestamp.forMillis(bigDec, null);
        checkFields(2012, 1, 1, 12, 12, 30, new BigDecimal("0.555123"), null, SECOND, ts);
        assertEquals("2012-01-01T12:12:30.555123-00:00", ts.toString());
        assertEquals("2012-01-01T12:12:30.555123Z", ts.toZString());
    }


    @SuppressWarnings("unused")
    @Test (expected = NullPointerException.class)
    public void testNewTimestampFromBigDecimalWithNull()
    {
        Timestamp ts = Timestamp.forMillis(null, PST_OFFSET);
    }

    /**
     * Test for {@link Timestamp#forMillis(BigDecimal, Integer)},
     * ensuring that varying BigDecimals with different scales produce
     * Timestamps (with correct precision of second/fractional seconds) as
     * expected.
     */
    @SuppressWarnings("deprecation")
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

        ts = Timestamp.forMillis(decScaleNegFour, null);
        checkFields(2012, 1, 1, 12, 12, 30, null, null, SECOND, ts);

        ts = Timestamp.forMillis(decScaleNegThree, null);
        checkFields(2012, 1, 1, 12, 12, 30, null, null, SECOND, ts);

        ts = Timestamp.forMillis(decScaleNegTwo, null);
        checkFields(2012, 1, 1, 12, 12, 30, new BigDecimal("0.5"), null, SECOND, ts);

        ts = Timestamp.forMillis(decScaleNegOne, null);
        checkFields(2012, 1, 1, 12, 12, 30, new BigDecimal("0.55"), null, SECOND, ts);

        ts = Timestamp.forMillis(decScaleZero, null);
        checkFields(2012, 1, 1, 12, 12, 30, new BigDecimal("0.555"), null, SECOND, ts);

        ts = Timestamp.forMillis(decScalePosOne, null);
        checkFields(2012, 1, 1, 12, 12, 30, new BigDecimal("0.5555"), null, SECOND, ts);

        ts = Timestamp.forMillis(new BigDecimal("0e3"), null);
        checkFields(1970, 1, 1, 0, 0, 0, null, null, SECOND, ts);

        ts = Timestamp.forMillis(new BigDecimal("1e3"), null);
        checkFields(1970, 1, 1, 0, 0, 1, null, null, SECOND, ts);
    }

    /** Test for {@link Timestamp#forMillis(long, Integer)} */
    @Test
    public void testNewTimestampFromLong()
    {
        long actualMillis = 1265019072340L;

        Timestamp ts = Timestamp.forMillis(actualMillis, PST_OFFSET);
        checkFields(2010, 2, 1, 2, 11, 12, new BigDecimal("0.340"), PST_OFFSET, SECOND, ts);
        assertEquals("2010-02-01T02:11:12.340-08:00", ts.toString());
        assertEquals("2010-02-01T10:11:12.340Z", ts.toZString());

        ts = Timestamp.forMillis(actualMillis, null);
        checkFields(2010, 2, 1, 10, 11, 12, new BigDecimal("0.340"), null, SECOND, ts);
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
        Precision p             = SECOND;

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

        Timestamp ts = Timestamp.forCalendar(cal);
        checkFields(2009, 1, 1, 0, 0, 0, null, UNKNOWN_OFFSET, YEAR, ts);
        assertEquals("2009T", ts.toString());
        assertEquals("2009T", ts.toZString());

        // ===== Timestamp month precision =====
        cal.clear();
        cal.set(Calendar.YEAR, 2009);
        cal.set(Calendar.MONTH, 2);
        assertFalse(cal.isSet(Calendar.HOUR_OF_DAY));

        ts = Timestamp.forCalendar(cal);
        checkFields(2009, 3, 1, 0, 0, 0, null, UNKNOWN_OFFSET, MONTH, ts);
        assertEquals("2009-03T", ts.toString());
        assertEquals("2009-03T", ts.toZString());

        // ===== Timestamp day precision =====
        cal.clear();
        cal.set(2009, 2, 18);
        assertFalse(cal.isSet(Calendar.HOUR_OF_DAY) &&
                    cal.isSet(Calendar.MINUTE));

        ts = Timestamp.forCalendar(cal);
        checkFields(2009, 3, 18, 0, 0, 0, null, UNKNOWN_OFFSET, DAY, ts);
        assertEquals("2009-03-18", ts.toString());
        assertEquals("2009-03-18", ts.toZString());

        // ===== Timestamp minute precision =====
        cal.clear();
        cal.set(2009, 1, 1, 10, 11);
        assertFalse(cal.isSet(Calendar.SECOND));

        ts = Timestamp.forCalendar(cal);
        checkFields(2009, 2, 1, 10, 11, 0, null, 0, MINUTE, ts);
        assertEquals("2009-02-01T10:11Z", ts.toString());
        assertEquals("2009-02-01T10:11Z", ts.toZString());

        // ===== Timestamp second precision =====
        cal.clear();
        cal.set(2009, 1, 1, 10, 11, 12);
        assertFalse(cal.isSet(Calendar.MILLISECOND));

        ts = Timestamp.forCalendar(cal);
        checkFields(2009, 2, 1, 10, 11, 12, null, 0, SECOND, ts);
        assertEquals("2009-02-01T10:11:12Z", ts.toString());
        assertEquals("2009-02-01T10:11:12Z", ts.toZString());

        // ===== Timestamp fractional second precision =====
        cal.clear();
        cal.set(2009, 1, 1, 10, 11, 12);
        cal.set(Calendar.MILLISECOND, 345);
        assertTrue(cal.isSet(Calendar.MILLISECOND));

        ts = Timestamp.forCalendar(cal);
        checkFields(2009, 2, 1, 10, 11, 12, new BigDecimal("0.345"), 0, SECOND, ts);
        assertEquals("2009-02-01T10:11:12.345Z", ts.toString());
        assertEquals("2009-02-01T10:11:12.345Z", ts.toZString());

        // ===== Timestamp local offset =====
        cal.clear();
        cal.set(2009, 1, 1, 10, 11, 12);
        cal.set(Calendar.MILLISECOND, 345);
        cal.setTimeZone(TimeZone.getTimeZone("PST"));
        assertEquals(TimeZone.getTimeZone("PST"), cal.getTimeZone());

        ts = Timestamp.forCalendar(cal);
        checkFields(2009, 2, 1, 10, 11, 12, new BigDecimal("0.345"), PST_OFFSET, SECOND, ts);
        assertEquals("2009-02-01T10:11:12.345-08:00", ts.toString());
        assertEquals("2009-02-01T18:11:12.345Z", ts.toZString());

        // ===== Timestamp zone offset =====
        cal.clear();
        cal.set(2009, 1, 1, 10, 11, 12);
        cal.set(Calendar.MILLISECOND, 345);
        cal.set(Calendar.ZONE_OFFSET, -28800000);

        ts = Timestamp.forCalendar(cal);
        checkFields(2009, 2, 1, 10, 11, 12, new BigDecimal("0.345"), PST_OFFSET, SECOND, ts);
        assertEquals("2009-02-01T10:11:12.345-08:00", ts.toString());
        assertEquals("2009-02-01T18:11:12.345Z", ts.toZString());
    }


    @Test
    public void testFromCalendarWithDaylightSavingsTime()
    {
        Calendar cal = makeUtcCalendar();
        cal.clear();
        cal.set(2012, 2, 9, 10, 11, 12);
        cal.set(Calendar.MILLISECOND, 345);
        cal.setTimeZone(TimeZone.getTimeZone("PST"));
        Timestamp ts = Timestamp.forCalendar(cal);
        checkFields(2012, 3, 9, 10, 11, 12, new BigDecimal("0.345"), PST_OFFSET, SECOND, ts);
        assertEquals("2012-03-09T10:11:12.345-08:00", ts.toString());

        // Move forward into daylight savings
        cal.add(Calendar.DAY_OF_MONTH, 5);
        ts = Timestamp.forCalendar(cal);
        checkFields(2012, 3, 14, 10, 11, 12, new BigDecimal("0.345"), -420, SECOND, ts);
        assertEquals("2012-03-14T10:11:12.345-07:00", ts.toString());
    }


    @Test(expected = IllegalArgumentException.class)
    public void testNewTimestampFromClearCalendar()
    {
        Calendar cal = makeUtcCalendar();
        cal.clear();
        Timestamp.forCalendar(cal);
    }

    @SuppressWarnings("deprecation")
    private void checkFraction(String textOfFrac, BigDecimal frac)
    {
        Timestamp expected =
            Timestamp.valueOf("2000-11-14T17:30:12" + textOfFrac + "Z");

        // negative fractions are not meaningful for checking second construction
        if (frac.signum() >= 0) {
            BigDecimal seconds = BigDecimal.valueOf(12).add(frac);
            Timestamp ts = Timestamp.forSecond(2000, 11, 14, 17, 30, seconds, 0);
            assertEquals(expected, ts);
            assertEquals("hash code", expected.hashCode(), ts.hashCode());
        }

        Timestamp ts = createFromUtcFields(SECOND, 2000, 11, 14, 17, 30, 12, frac,
                                 UTC_OFFSET);
        assertEquals(expected, ts);
        assertEquals("hash code", expected.hashCode(), ts.hashCode());
    }

    @Test
    public void testTimestampWithDecimalFraction()
    {
        checkFraction(".00", Decimal.valueOf(".00"));
        checkFraction(".00", new BigDecimal(".00"));

        checkFraction(".345", Decimal.valueOf(".345"));
        checkFraction(".345", new BigDecimal (".345"));

        checkFraction(".123", Decimal.valueOf("-0.123"));
        checkFraction(".123", new BigDecimal ("-0.123"));
    }

    @Test
    public void testNewTimestampWithLargeFraction()
    {
        BigDecimal frac = new BigDecimal("1.23");

        try {
            Timestamp.createFromUtcFields(SECOND,
                                          2000, 11, 14, 17, 30, 12, frac, 0);
            fail("Expected exception");
        }
        catch (IllegalArgumentException e) { }
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

        BigDecimal second = BigDecimal.valueOf(ts.getSecond());
        BigDecimal frac = ts.getDecimalSecond().subtract(second);
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
    @SuppressWarnings("deprecation")
    public void testTimestampCopyConstructor()
    {
        BigDecimal second = new BigDecimal("12.3456789");
        Timestamp expectedTs =
            Timestamp.forSecond(2010, 2, 1, 10, 11, second, PST_OFFSET);
        assertEquals("2010-02-01T10:11:12.3456789-08:00",
                     expectedTs.toString());

        //===== Test on Timestamp(...) fractional precision copy-constructor =====
        Timestamp actualTs = Timestamp.forSecond(expectedTs.getYear(),
                                                 expectedTs.getMonth(),
                                                 expectedTs.getDay(),
                                                 expectedTs.getHour(),
                                                 expectedTs.getMinute(),
                                                 expectedTs.getDecimalSecond(),
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
     * Ensures that a point in time represented by a
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
            Timestamp.forMillis(negativeEpochDecimalMillis, UTC_OFFSET);
        long expectedMillis = expectedTs.getMillis();
        Timestamp expectedTsMillis =
            Timestamp.forMillis(expectedMillis, UTC_OFFSET);
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
        Timestamp actualTs = Timestamp.forMillis(negativeEpochDecimalMillis, UTC_OFFSET);
        assertEquals(expectedTs, actualTs);


        //===== Test on Timestamp(long, Integer) constructor =====
        actualTs = Timestamp.forMillis(expectedMillis, UTC_OFFSET);
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


    /**
     * Timestamps with precision coarser than minutes always have unknown
     * local offset.
     */
    @Test
    public void testCreateWithBogusLocalOffset()
        throws Exception
    {
        Timestamp ts = createFromUtcFields(Precision.DAY, 2014, 4, 25,
                                           1, 2, 3, null,
                                           60);
        assertEquals(Timestamp.valueOf("2014-04-25"), ts);

        ts = createFromUtcFields(Precision.MONTH, 2014, 4, 25,
                                 1, 2, 3, null,
                                 60);
        assertEquals(Timestamp.valueOf("2014-04T"), ts);

        ts = createFromUtcFields(Precision.YEAR, 2014, 4, 25,
                                 1, 2, 3, null,
                                 60);
        assertEquals(Timestamp.valueOf("2014T"), ts);
    }


    private Timestamp checkWithLocalOffset(Timestamp orig, Integer offset,
                                           Timestamp expected)
        throws Exception
    {
        Timestamp actual = orig.withLocalOffset(offset);
        assertEquals(0, actual.compareTo(orig));         // same point-in-time
        assertEquals(orig.getPrecision(), actual.getPrecision());
        assertEquals(expected, actual);

        return actual;
    }

    Timestamp checkWithLocalOffset(Timestamp orig, Integer offset,
                                   String expected)
        throws Exception
    {
        Timestamp e = Timestamp.valueOf(expected);
        return checkWithLocalOffset(orig, offset, e);
    }

    @Test
    public void testWithLocalOffset()
        throws Exception
    {
        Timestamp ts = Timestamp.valueOf("2014-04-25T13:50:12.34Z");

        checkWithLocalOffset(ts, UTC_OFFSET, ts); // No change

        Timestamp ts2 =
            checkWithLocalOffset(ts, null, "2014-04-25T13:50:12.34-00:00");

        ts2 = checkWithLocalOffset(ts2, UTC_OFFSET, ts);
        ts2 = checkWithLocalOffset(ts2, -10, "2014-04-25T13:40:12.34-00:10");
        ts2 = checkWithLocalOffset(ts2,  10, "2014-04-25T14:00:12.34+00:10");


        // Seconds precision
        ts = Timestamp.valueOf("2014-04-25T13:50:12Z");

        checkWithLocalOffset(ts, UTC_OFFSET, ts); // No change

        ts2 = checkWithLocalOffset(ts, null, "2014-04-25T13:50:12-00:00");
        ts2 = checkWithLocalOffset(ts2, UTC_OFFSET, ts);
        ts2 = checkWithLocalOffset(ts2, -10, "2014-04-25T13:40:12-00:10");
        ts2 = checkWithLocalOffset(ts2,  10, "2014-04-25T14:00:12+00:10");


        // Minutes precision
        ts = Timestamp.valueOf("2014-04-25T13:50Z");

        checkWithLocalOffset(ts, UTC_OFFSET, ts); // No change

        ts2 = checkWithLocalOffset(ts, null, "2014-04-25T13:50-00:00");
        ts2 = checkWithLocalOffset(ts2, UTC_OFFSET, ts);
        ts2 = checkWithLocalOffset(ts2, -10, "2014-04-25T13:40-00:10");
        ts2 = checkWithLocalOffset(ts2,  10, "2014-04-25T14:00+00:10");


        // Not sure if this is meaningful since non-GMT timestamps must have
        // at least minutes precision.

        ts = Timestamp.valueOf("2014-04-25T");
        assertNull(ts.getLocalOffset());         // A reminder to the reader.

        ts2 = checkWithLocalOffset(ts, null,       ts);  // No change
        ts2 = checkWithLocalOffset(ts, UTC_OFFSET, ts);
        ts2 = checkWithLocalOffset(ts, -10,        ts);
        ts2 = checkWithLocalOffset(ts,  10,        ts);


        ts = Timestamp.valueOf("2014-04T");
        assertNull(ts.getLocalOffset());         // A reminder to the reader.

        ts2 = checkWithLocalOffset(ts, null,       ts);  // No change
        ts2 = checkWithLocalOffset(ts, UTC_OFFSET, ts);
        ts2 = checkWithLocalOffset(ts, -10,        ts);
        ts2 = checkWithLocalOffset(ts,  10,        ts);


        ts = Timestamp.valueOf("2014T");
        assertNull(ts.getLocalOffset());         // A reminder to the reader.

        ts2 = checkWithLocalOffset(ts, null,       ts);  // No change
        ts2 = checkWithLocalOffset(ts, UTC_OFFSET, ts);
        ts2 = checkWithLocalOffset(ts, -10,        ts);
        ts2 = checkWithLocalOffset(ts,  10,        ts);
    }


    //=========================================================================
    // Timestamp arithmetic


    private void addYear(String orig, int amount, String expected)
    {
        Timestamp ts1 = Timestamp.valueOf(orig);
        Timestamp ts2 = ts1.addYear(amount);
        checkTimestamp(expected, ts2);
    }

    private void addYear(String orig, int amount, String expected,
                         String suffix)
    {
        addYear(orig + suffix, amount, expected + suffix);
    }

    private void addYearWithOffsets(String orig, int amount, String expected,
                                    String suffix)
    {
        addYear(orig, amount, expected, suffix + "-00:00");
        addYear(orig, amount, expected, suffix + "Z");
        addYear(orig, amount, expected, suffix + "+01:23");
        addYear(orig, amount, expected, suffix + "-01:23");
        addYear(orig, amount, expected, suffix + "+23:59");
        addYear(orig, amount, expected, suffix + "-23:59");
    }

    private void addYearWithMins(String orig, int amount, String expected)
    {
        addYear(orig, amount, expected);

        addYearWithOffsets(orig, amount, expected, "T19:03");
        addYearWithOffsets(orig, amount, expected, "T19:03:23");
        addYearWithOffsets(orig, amount, expected, "T19:03:23.0");
        addYearWithOffsets(orig, amount, expected, "T19:03:23.00");
        addYearWithOffsets(orig, amount, expected, "T19:03:23.000");
        addYearWithOffsets(orig, amount, expected, "T19:03:23.456");
        addYearWithOffsets(orig, amount, expected, "T19:03:23.0000");
        addYearWithOffsets(orig, amount, expected, "T19:03:23.45678");
    }

    @Test
    public void testAddYear()
    {
        addYear("2012T",     1, "2013T");
        addYear("2012T",    -1, "2011T");

        addYear("2012-04T",  1, "2013-04T");
        addYear("2012-04T", -1, "2011-04T");

        addYearWithMins("2012-04-23",  1, "2013-04-23");
        addYearWithMins("2012-04-23", -1, "2011-04-23");

        // Leap-year handling
        addYearWithMins("2012-02-29", -5, "2007-02-28");
        addYearWithMins("2012-02-29", -4, "2008-02-29");
        addYearWithMins("2012-02-29", -1, "2011-02-28");
        addYearWithMins("2012-02-29",  1, "2013-02-28");
        addYearWithMins("2012-02-29",  4, "2016-02-29");
        addYearWithMins("2012-02-29",  5, "2017-02-28");
    }

    //-------------------------------------------------------------------------

    private void addMonth(String orig, int amount, String expected)
    {
        Timestamp ts1 = Timestamp.valueOf(orig);
        Timestamp ts2 = ts1.addMonth(amount);
        checkTimestamp(expected, ts2);
    }

    private void addMonth(String orig, int amount, String expected,
                          String suffix)
    {
        addMonth(orig + suffix, amount, expected + suffix);
    }

    private void addMonthWithOffsets(String orig, int amount, String expected,
                                     String suffix)
    {
        addMonth(orig, amount, expected, suffix + "-00:00");
        addMonth(orig, amount, expected, suffix + "Z");
        addMonth(orig, amount, expected, suffix + "+01:23");
        addMonth(orig, amount, expected, suffix + "-01:23");
        addMonth(orig, amount, expected, suffix + "+23:59");
        addMonth(orig, amount, expected, suffix + "-23:59");
    }

    private void addMonthWithMins(String orig, int amount, String expected)
    {
        addMonth(orig, amount, expected);

        addMonthWithOffsets(orig, amount, expected, "T19:03");
        addMonthWithOffsets(orig, amount, expected, "T19:03:23");
        addMonthWithOffsets(orig, amount, expected, "T19:03:23.0");
        addMonthWithOffsets(orig, amount, expected, "T19:03:23.00");
        addMonthWithOffsets(orig, amount, expected, "T19:03:23.000");
        addMonthWithOffsets(orig, amount, expected, "T19:03:23.456");
        addMonthWithOffsets(orig, amount, expected, "T19:03:23.0000");
        addMonthWithOffsets(orig, amount, expected, "T19:03:23.45678");
    }

    @Test
    public void testAddMonth()
    {
        // TODO Is it reasonable to add months to TSs that aren't that precise?
        addMonth("2012T",    -1, "2011T");
        addMonth("2012T",     1, "2012T");

        addMonth("2012-04T", -4, "2011-12T");
        addMonth("2012-04T", -1, "2012-03T");
        addMonth("2012-04T",  1, "2012-05T");
        addMonth("2012-04T",  9, "2013-01T");

        addMonthWithMins("2012-04-23", -4, "2011-12-23");
        addMonthWithMins("2012-04-23", -1, "2012-03-23");
        addMonthWithMins("2012-04-23",  1, "2012-05-23");
        addMonthWithMins("2012-04-23",  9, "2013-01-23");

        addMonthWithMins("2011-01-31",  1, "2011-02-28");
        addMonthWithMins("2011-02-28", 12, "2012-02-28");
        addMonthWithMins("2012-01-31",  1, "2012-02-29");
        addMonthWithMins("2012-01-31",  2, "2012-03-31");
        addMonthWithMins("2012-01-31",  3, "2012-04-30");
        addMonthWithMins("2012-02-29", -1, "2012-01-29");
        addMonthWithMins("2012-02-29", 12, "2013-02-28");
        addMonthWithMins("2012-02-29", 24, "2014-02-28");
        addMonthWithMins("2012-02-29", 48, "2016-02-29");
        addMonthWithMins("2013-01-31",-11, "2012-02-29");
    }


    //-------------------------------------------------------------------------

    private void addDay(String orig, int amount, String expected)
    {
        Timestamp ts1 = Timestamp.valueOf(orig);
        Timestamp ts2 = ts1.addDay(amount);
        checkTimestamp(expected, ts2);
    }

    private void addDay(String orig, int amount, String expected,
                          String suffix)
    {
        addDay(orig + suffix, amount, expected + suffix);
    }

    private void addDayWithOffsets(String orig, int amount, String expected,
                                     String suffix)
    {
        addDay(orig, amount, expected, suffix + "-00:00");
        addDay(orig, amount, expected, suffix + "Z");
        addDay(orig, amount, expected, suffix + "+01:23");
        addDay(orig, amount, expected, suffix + "-01:23");
        addDay(orig, amount, expected, suffix + "+23:59");
        addDay(orig, amount, expected, suffix + "-23:59");
    }

    private void addDayWithMins(String orig, int amount, String expected)
    {
        addDay(orig, amount, expected);

        addDayWithOffsets(orig, amount, expected, "T19:03");
        addDayWithOffsets(orig, amount, expected, "T19:03:23");
        addDayWithOffsets(orig, amount, expected, "T19:03:23.0");
        addDayWithOffsets(orig, amount, expected, "T19:03:23.00");
        addDayWithOffsets(orig, amount, expected, "T19:03:23.000");
        addDayWithOffsets(orig, amount, expected, "T19:03:23.456");
        addDayWithOffsets(orig, amount, expected, "T19:03:23.0000");
        addDayWithOffsets(orig, amount, expected, "T19:03:23.45678");
    }

    @Test
    public void testAddDay()
    {
        // TODO Is it reasonable to add days to TSs that aren't that precise?
        addDay("2012T",     1, "2012T");
        addDay("2012T",    -1, "2011T");

        addDay("2012-04T",-32, "2012-02T");
        addDay("2012-04T",-31, "2012-03T");
        addDay("2012-04T", -1, "2012-03T");
        addDay("2012-04T",  1, "2012-04T");
        addDay("2012-04T",  9, "2012-04T");
        addDay("2012-04T", 30, "2012-05T");
        addDay("2012-04T", 31, "2012-05T");


        addDayWithMins("2011-01-31",-31, "2010-12-31");
        addDayWithMins("2011-01-31",-30, "2011-01-01");
        addDayWithMins("2011-01-31",  1, "2011-02-01");
        addDayWithMins("2011-01-31", 28, "2011-02-28");
        addDayWithMins("2011-01-31", 29, "2011-03-01");
        addDayWithMins("2011-01-31", 30, "2011-03-02");
        addDayWithMins("2012-01-31",  1, "2012-02-01");
        addDayWithMins("2012-01-31", 28, "2012-02-28");
        addDayWithMins("2012-01-31", 29, "2012-02-29");
        addDayWithMins("2012-01-31", 30, "2012-03-01");
        addDayWithMins("2012-03-01",-30, "2012-01-31");
    }

    //-------------------------------------------------------------------------

    private void addHour(String orig, int amount, String expected)
    {
        Timestamp ts1 = Timestamp.valueOf(orig);
        Timestamp ts2 = ts1.addHour(amount);
        checkTimestamp(expected, ts2);
    }

    private void addHour(String orig, int amount, String expected,
                         String suffix)
    {
        addHour(orig + suffix, amount, expected + suffix);
    }

    private void addHourWithOffsets(String orig, int amount, String expected,
                                    String suffix)
    {
        addHour(orig, amount, expected, suffix + "-00:00");
        addHour(orig, amount, expected, suffix + "Z");
        addHour(orig, amount, expected, suffix + "+01:23");
        addHour(orig, amount, expected, suffix + "-01:23");
        addHour(orig, amount, expected, suffix + "+23:59");
        addHour(orig, amount, expected, suffix + "-23:59");
    }

    private void addHourWithMins(String orig, int amount, String expected)
    {
        addHourWithOffsets(orig, amount, expected, ":03");
        addHourWithOffsets(orig, amount, expected, ":03:23");
        addHourWithOffsets(orig, amount, expected, ":03:23.0");
        addHourWithOffsets(orig, amount, expected, ":03:23.00");
        addHourWithOffsets(orig, amount, expected, ":03:23.000");
        addHourWithOffsets(orig, amount, expected, ":03:23.456");
        addHourWithOffsets(orig, amount, expected, ":03:23.0000");
        addHourWithOffsets(orig, amount, expected, ":03:23.45678");
    }

    @Test
    public void testAddHour()
    {
        // TODO Is it reasonable to add hours to TSs that aren't that precise?
        addHour("2012T",    -1, "2011T");
        addHour("2012T",     0, "2012T");
        addHour("2012T",     1, "2012T");

        addHour("2012-04T", -31 * 24 - 1, "2012-02T");
        addHour("2012-04T", -31 * 24    , "2012-03T");
        addHour("2012-04T",           -1, "2012-03T");
        addHour("2012-04T",            0, "2012-04T");
        addHour("2012-04T",            1, "2012-04T");
        addHour("2012-04T",  30 * 24 - 1, "2012-04T");
        addHour("2012-04T",  30 * 24    , "2012-05T");

        addHour("2011-02-28",  -1, "2011-02-27");
        addHour("2011-02-28",   0, "2011-02-28");
        addHour("2011-02-28",   1, "2011-02-28");
        addHour("2011-02-28",  24, "2011-03-01");
        addHour("2011-03-01", -24, "2011-02-28");
        addHour("2012-02-28",  24, "2012-02-29");
        addHour("2012-02-29", -24, "2012-02-28");
        addHour("2012-02-29",  24, "2012-03-01");

        addHour("2012-10-04",-48, "2012-10-02");
        addHour("2012-10-04",-24, "2012-10-03");
        addHour("2012-10-04", -1, "2012-10-03");
        addHour("2012-10-04",  1, "2012-10-04");
        addHour("2012-10-04", 24, "2012-10-05");
        addHour("2012-10-04", 48, "2012-10-06");


        addHourWithMins("2011-01-31T12",  0, "2011-01-31T12");
        addHourWithMins("2011-01-31T12", 12, "2011-02-01T00");

        addHourWithMins("2011-02-28T02", 25, "2011-03-01T03");
        addHourWithMins("2012-02-28T02", 25, "2012-02-29T03");

        addHourWithMins("2011-03-01T02", -4, "2011-02-28T22");
        addHourWithMins("2012-03-01T02", -4, "2012-02-29T22");
    }

    //-------------------------------------------------------------------------

    private void addMinute(String orig, int amount, String expected)
    {
        Timestamp ts1 = Timestamp.valueOf(orig);
        Timestamp ts2 = ts1.addMinute(amount);
        checkTimestamp(expected, ts2);
    }

    private void addMinute(String orig, int amount, String expected,
                           String suffix)
    {
        addMinute(orig + suffix, amount, expected + suffix);
    }

    private void addMinuteWithOffsets(String orig, int amount, String expected,
                                      String suffix)
    {
        addMinute(orig, amount, expected, suffix + "-00:00");
        addMinute(orig, amount, expected, suffix + "Z");
        addMinute(orig, amount, expected, suffix + "+01:23");
        addMinute(orig, amount, expected, suffix + "-01:23");
        addMinute(orig, amount, expected, suffix + "+23:59");
        addMinute(orig, amount, expected, suffix + "-23:59");
    }

    private void addMinuteWithsSecs(String orig, int amount, String expected)
    {
        addMinuteWithOffsets(orig, amount, expected, "");
        addMinuteWithOffsets(orig, amount, expected, ":23");
        addMinuteWithOffsets(orig, amount, expected, ":23.0");
        addMinuteWithOffsets(orig, amount, expected, ":23.00");
        addMinuteWithOffsets(orig, amount, expected, ":23.000");
        addMinuteWithOffsets(orig, amount, expected, ":23.456");
        addMinuteWithOffsets(orig, amount, expected, ":23.0000");
        addMinuteWithOffsets(orig, amount, expected, ":23.45678");
    }

    @Test
    public void testAddMinute()
    {
        // TODO Is it reasonable to add minutes to TSs that aren't that precise?
        addMinute("2012T",    -1, "2011T");
        addMinute("2012T",     0, "2012T");
        addMinute("2012T",     1, "2012T");

        addMinute("2012-04T", -31 * 24 * 60 - 1, "2012-02T");
        addMinute("2012-04T", -31 * 24 * 60    , "2012-03T");
        addMinute("2012-04T",                -1, "2012-03T");
        addMinute("2012-04T",                 0, "2012-04T");
        addMinute("2012-04T",                 1, "2012-04T");
        addMinute("2012-04T",  30 * 24 * 60 - 1, "2012-04T");
        addMinute("2012-04T",  30 * 24 * 60    , "2012-05T");

        addMinute("2011-02-28",       -1, "2011-02-27");
        addMinute("2011-02-28",        0, "2011-02-28");
        addMinute("2011-02-28",        1, "2011-02-28");
        addMinute("2011-02-28",  24 * 60, "2011-03-01");
        addMinute("2011-03-01", -24 * 60, "2011-02-28");
        addMinute("2012-02-28",  24 * 60, "2012-02-29");
        addMinute("2012-02-29", -24 * 60, "2012-02-28");
        addMinute("2012-02-29",  24 * 60, "2012-03-01");

        addMinute("2012-10-04", -48 * 60, "2012-10-02");
        addMinute("2012-10-04", -24 * 60, "2012-10-03");
        addMinute("2012-10-04",       -1, "2012-10-03");
        addMinute("2012-10-04",        1, "2012-10-04");
        addMinute("2012-10-04",  24 * 60, "2012-10-05");
        addMinute("2012-10-04",  48 * 60, "2012-10-06");



        addMinuteWithsSecs("2011-01-31T12:03",  -60, "2011-01-31T11:03");
        addMinuteWithsSecs("2011-01-31T12:03",    0, "2011-01-31T12:03");
        addMinuteWithsSecs("2011-01-31T12:03",   60, "2011-01-31T13:03");
        addMinuteWithsSecs("2011-01-31T23:03",   57, "2011-02-01T00:00");

        addMinuteWithsSecs("2011-02-28T02:03", 25 * 60, "2011-03-01T03:03");
        addMinuteWithsSecs("2012-02-28T02:03", 25 * 60, "2012-02-29T03:03");

        addMinuteWithsSecs("2011-03-01T02:35", -4 * 60, "2011-02-28T22:35");
        addMinuteWithsSecs("2012-03-01T02:35", -4 * 60, "2012-02-29T22:35");
    }


    //-------------------------------------------------------------------------

    private void addSecond(String orig, int amount, String expected)
    {
        Timestamp ts1 = Timestamp.valueOf(orig);
        Timestamp ts2 = ts1.addSecond(amount);
        checkTimestamp(expected, ts2);
    }

    private void addSecond(String orig, int amount, String expected,
                           String suffix)
    {
        addSecond(orig + suffix, amount, expected + suffix);
    }

    private void addSecondWithOffsets(String orig, int amount, String expected,
                                      String suffix)
    {
        addSecond(orig, amount, expected, suffix + "-00:00");
        addSecond(orig, amount, expected, suffix + "Z");
        addSecond(orig, amount, expected, suffix + "+01:23");
        addSecond(orig, amount, expected, suffix + "-01:23");
        addSecond(orig, amount, expected, suffix + "+23:59");
        addSecond(orig, amount, expected, suffix + "-23:59");
    }

    private void addSecondWithsFrac(String orig, int amount, String expected)
    {
        addSecondWithOffsets(orig, amount, expected, "");
        addSecondWithOffsets(orig, amount, expected, ".0");
        addSecondWithOffsets(orig, amount, expected, ".00");
        addSecondWithOffsets(orig, amount, expected, ".000");
        addSecondWithOffsets(orig, amount, expected, ".456");
        addSecondWithOffsets(orig, amount, expected, ".0000");
        addSecondWithOffsets(orig, amount, expected, ".45678");
    }

    @Test
    public void testAddSecond()
    {
        // TODO Is it reasonable to add seconds to TSs that aren't that precise?
        addSecond("2012T",    -1, "2011T");
        addSecond("2012T",     0, "2012T");
        addSecond("2012T",     1, "2012T");

        addSecond("2012-04T", -31 * 24 * 60 * 60 - 1, "2012-02T");
        addSecond("2012-04T", -31 * 24 * 60 * 60    , "2012-03T");
        addSecond("2012-04T",                     -1, "2012-03T");
        addSecond("2012-04T",                      0, "2012-04T");
        addSecond("2012-04T",                      1, "2012-04T");
        addSecond("2012-04T",  30 * 24 * 60 * 60 - 1, "2012-04T");
        addSecond("2012-04T",  30 * 24 * 60 * 60    , "2012-05T");


        addSecond("2011-02-28",            -1, "2011-02-27");
        addSecond("2011-02-28",             0, "2011-02-28");
        addSecond("2011-02-28",             1, "2011-02-28");
        addSecond("2011-02-28",  24 * 60 * 60, "2011-03-01");
        addSecond("2011-03-01", -24 * 60 * 60, "2011-02-28");
        addSecond("2012-02-28",  24 * 60 * 60, "2012-02-29");
        addSecond("2012-02-29", -24 * 60 * 60, "2012-02-28");
        addSecond("2012-02-29",  24 * 60 * 60, "2012-03-01");

        addSecond("2012-10-04", -24 * 60 * 60 - 1, "2012-10-02");
        addSecond("2012-10-04", -24 * 60 * 60    , "2012-10-03");
        addSecond("2012-10-04",                -1, "2012-10-03");
        addSecond("2012-10-04",                 1, "2012-10-04");
        addSecond("2012-10-04",  48 * 60 * 60 - 1, "2012-10-05");
        addSecond("2012-10-04",  48 * 60 * 60    , "2012-10-06");


        addSecondWithsFrac("2011-01-31T12:03:23",  -60 * 60, "2011-01-31T11:03:23");
        addSecondWithsFrac("2011-01-31T12:03:23",         0, "2011-01-31T12:03:23");
        addSecondWithsFrac("2011-01-31T12:03:23",   60 * 60, "2011-01-31T13:03:23");
        addSecondWithsFrac("2011-01-31T23:03:23",   57 * 60, "2011-02-01T00:00:23");

        addSecondWithsFrac("2011-02-28T02:03:23", 25 * 60 * 60, "2011-03-01T03:03:23");
        addSecondWithsFrac("2012-02-28T02:03:23", 25 * 60 * 60, "2012-02-29T03:03:23");

        addSecondWithsFrac("2011-03-01T02:35:23", -4 * 60 * 60, "2011-02-28T22:35:23");
        addSecondWithsFrac("2012-03-01T02:35:23", -4 * 60 * 60, "2012-02-29T22:35:23");
    }

    @Test
    public void forYear()
    {
        checkTimestamp("2016T", Timestamp.forYear(2016));
    }

    @Test(expected = IllegalArgumentException.class)
    public void forYearRejectsZeroYear()
    {
        Timestamp.forYear(0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forYearRejectsNegativeYear()
    {
        Timestamp.forYear(-999);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forYearRejectsLargeYear()
    {
        Timestamp.forYear(10000);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forYearRejectsYearWithUpperBitsSet()
    {
        Timestamp.forYear(0x7FFFFF00 + 2016);
    }

    @Test
    public void forMonth()
    {
        checkTimestamp("2016-03T", Timestamp.forMonth(2016, 3));
    }

    @Test(expected = IllegalArgumentException.class)
    public void forMonthRejectsZeroYear()
    {
        Timestamp.forMonth(0,  1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forMonthRejectsNegativeYear()
    {
        Timestamp.forMonth(-123, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forMonthRejectsLargeYear()
    {
        Timestamp.forMonth(12345, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forMonthRejectsYearWithUpperBitsSet()
    {
        Timestamp.forMonth(0x7FFFFF00 + 2016, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forMonthRejectsZeroMonth()
    {
        Timestamp.forMonth(2016, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forMonthRejectsNegativeMonth()
    {
        Timestamp.forMonth(2016, -12);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forMonthRejectsLargeMonth()
    {
        Timestamp.forMonth(2016, 13);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forMonthRejectsMonthWithUpperBitsSet()
    {
        Timestamp.forMonth(2016, 0x7FFFFF00 + 3);
    }

    @Test
    public void forDay()
    {
        checkTimestamp("2016-03-31", Timestamp.forDay(2016, 3, 31));
    }

    @Test(expected = IllegalArgumentException.class)
    public void forDayRejectsZeroYear()
    {
        Timestamp.forDay(0, 3, 31);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forDayRejectsNegativeYear()
    {
        Timestamp.forDay(-999, 3, 31);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forDayRejectsLargeYear()
    {
        Timestamp.forDay(10000, 3, 31);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forDayRejectsYearWithUpperBitsSet()
    {
        Timestamp.forDay(0x7FFFFF00 + 2016, 3, 31);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forDayRejectsZeroMonth()
    {
        Timestamp.forDay(2016, 0, 31);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forDayRejectsNegativeMonth()
    {
        Timestamp.forDay(2016, -12, 31);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forDayRejectsLargeMonth()
    {
        Timestamp.forDay(2016, 13, 31);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forDayRejectsMonthWithUpperBitsSet()
    {
        Timestamp.forDay(2016, 0x7FFFFF00 + 3, 31);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forDayRejectsZeroDay()
    {
        Timestamp.forDay(2016, 03, 0);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forDayRejectsNegativeDay()
    {
        Timestamp.forDay(2016, 03, -888);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forDayRejectsLargeDay()
    {
        Timestamp.forDay(2016, 03, 32);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forDayRejectsDayWithUpperBitsSet()
    {
        Timestamp.forDay(2016, 03, 0x7FFFFF00 + 31);
    }

    @Test(expected = IllegalArgumentException.class)
    public void forCalendarRejectsLargeYear()
    {
        int year = 10000;

        Calendar cal = Calendar.getInstance();
        cal.set(year, 1, 1);

        Timestamp t = Timestamp.forCalendar(cal);
        assertEquals(year, t.getYear());
    }
}
