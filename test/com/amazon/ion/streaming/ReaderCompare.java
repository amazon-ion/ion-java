/*
 * Copyright (c) 2008-2009 Amazon.com, Inc.  All rights reserved.
 */

package com.amazon.ion.streaming;

import static com.amazon.ion.impl.IonImplUtils.READER_HASNEXT_REMOVED;

import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.Timestamp;
import java.math.BigDecimal;
import org.junit.Assert;

/**
 *
 */
public class ReaderCompare
    extends Assert
{
    public static void compare(IonReader it1, IonReader it2) {
        while (hasNext(it1, it2)) {
            IonType t1 = (READER_HASNEXT_REMOVED ? it1.getType() : it1.next());
            IonType t2 = (READER_HASNEXT_REMOVED ? it2.getType() : it2.next());

            assertEquals("ion type", t1, t2);
            if (t1 == null) break;

            if (it1.isInStruct()) {
                compareFieldNames(it1, it2);
            }
            compareAnnotations(it1, it2);
            assertEquals(it1.isNullValue(), it2.isNullValue());
            if (it1.isNullValue()) {
                // remember - anything can be a null value
                continue;
            }

            switch (t1) {
                case NULL:
                    assertTrue(it1.isNullValue());
                    assertTrue(it2.isNullValue());
                    break;
                case BOOL:
                case INT:
                case FLOAT:
                case DECIMAL:
                case TIMESTAMP:
                case STRING:
                case SYMBOL:
                case BLOB:
                case CLOB:
                    compareScalars(t1, it1, it2);
                    break;
                case STRUCT:
                case LIST:
                case SEXP:
                    it1.stepIn();
                    it2.stepIn();
                    compare(it1, it2);
                    it1.stepOut();
                    it2.stepOut();
                    break;

                default:
                    throw new IllegalStateException("iterated to a type that's not expected");
            }
        }
        assertFalse(hasNext(it1, it2));
    }


    public static boolean hasNext(IonReader it1, IonReader it2)
    {
        boolean more;
        if (READER_HASNEXT_REMOVED)
        {
            more = (it1.next() != null);
            assertEquals("next results don't match", more, it2.next() != null);
        }
        else
        {
            more = it1.hasNext();
            assertEquals("hasNext results don't match", more, it2.hasNext());

            // Check that result doesn't change
            assertEquals(more, it1.hasNext());
            assertEquals(more, it2.hasNext());
        }

        if (!more) {
            assertEquals(null, it1.next());
            assertEquals(null, it2.next());
        }

        return more;
    }

    public static void compareNonNullStrings(String what, String s1, String s2)
    {
        assertNotNull(what, s1);
        assertNotNull(what, s2);
        assertEquals(what, s1, s2);
    }

    public static void compareFieldNames(IonReader it1, IonReader it2) {
        String f1 = it1.getFieldName();
        String f2 = it2.getFieldName();
        compareNonNullStrings("field name", f1, f2);
        assertNotNull(f1);
        assertNotNull(f2);
        assertEquals(f1, f2);
    }

    public static void compareAnnotations(IonReader it1, IonReader it2) {
        String[] a1 = it1.getTypeAnnotations();
        String[] a2 = it2.getTypeAnnotations();
        if (a1 == null) {
            assertNull(a2);
        }
        else {
            assertEquals("annotation count", a1.length, a2.length);
            for (int ii=0; ii<a1.length; ii++) {
                String s1 = a1[ii];
                String s2 = a2[ii];
                compareNonNullStrings("annotation", s1, s2);
                assert s1 != null && s2 != null;
                assert s1.equals(s2);
            }
        }
    }

    public static void compareScalars(IonType t, IonReader it1, IonReader it2) {
        switch (t) {
            case BOOL:
                assertEquals(it1.booleanValue(), it2.booleanValue());
                break;
            case INT:
                assertEquals(it1.longValue(), it2.longValue());
                break;
            case FLOAT: {
                double v1 = it1.doubleValue();
                double v2 = it2.doubleValue();
                assertEquals(v1, v2, 0);
                // The last param is a delta, and we want exact match.
                break;
            }
            case DECIMAL:
                BigDecimal bd1 = it1.bigDecimalValue();
                BigDecimal bd2 = it2.bigDecimalValue();
                assertEquals(bd1, bd2);
                break;
            case TIMESTAMP:
                Timestamp t1 = it1.timestampValue();
                Timestamp t2 = it2.timestampValue();
                assertEquals(t1, t2);
                break;
            case STRING:
            case SYMBOL:
                String s1 = it1.stringValue();
                String s2 = it2.stringValue();
                assertEquals(s1, s2);
                break;
            case BLOB:
            case CLOB:
                byte[] b1 = it1.newBytes();
                byte[] b2 = it2.newBytes();
                assert b1 != null && b2 != null;
                assert b1.length == b2.length;
                for (int ii=0; ii<b1.length; ii++) {
                    byte v1 = b1[ii];
                    byte v2 = b2[ii];
                    assertEquals(v1, v2);
                }
                break;
            default:
                throw new IllegalStateException("iterated to a type that's not expected");
        }
    }
}
