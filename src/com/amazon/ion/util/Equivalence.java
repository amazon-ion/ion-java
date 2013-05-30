// Copyright (c) 2008-2012 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion.util;

import com.amazon.ion.Decimal;
import com.amazon.ion.IonBool;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonLob;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSymbol;
import com.amazon.ion.IonText;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Provides strict equivalence between two Ion Values.
 *
 * <p>
 * Basic usage of this class is as follows.
 *
 * <pre>
 *   IonValue v1 = ...;
 *   IonValue v2 = ...;
 *   com.amazon.ion.util.Equivalence.ionEquals( v1, v2 );
 * </pre>
 *
 * More likely, a static import would make using this class easier.
 *
 * <pre>
 *   import static com.amazon.authority.ion.Equivalence.ionEquals;
 *   ...
 *   boolean equivalent = ionEquals( v1, v2 );
 * </pre>
 *
 * </p>
 *
 * <p>
 * <h3>Ion Equivalence</h3>
 * In order to make Ion a useful model to describe data, we must first define
 * the notion of equivalence for all values representable in Ion. Equivalence
 * with respect to Ion values mean that if two Ion values, X and Y, are
 * equivalent, they represent the same data and can be substituted for the other
 * without loss of information.
 *
 * This relationship is:
 * <ul>
 * <li> symmetric: X is equivalent to Y if and only if Y is equivalent to X.
 * </li>
 * <li> transitive: if X is equivalent to Y and Y is equivalent to Z, then X is
 * equivalent to Z. </li>
 * <li> reflexive: X is equivalent to X. </li>
 * </ul>
 *
 * <h4>Ordered Sequence Equivalence</h4>
 * When an ordered sequence (i.e. tuple) of elements is specified in this
 * document, equivalence over such an ordered sequence is defined as follows.
 *
 * A tuple, A = (a1, a2, ..., an), is equivalent to another tuple, B = (b1, b2,
 * ..., bm) if and only if the cardinality (number of elements) of A equals the
 * cardinality of B (i.e. n == m) and ai is equivalent to bi for i = 1 ... n.
 *
 * <h4>Un-Ordered Sequence Equivalence</h4>
 * When an un-ordered sequence (i.e. bag or multi-set) is specified in this
 * document, equivalence over such a sequence is defined as follows.
 *
 * A bag, A = {a1, a2, ..., an} is equivalent to B = {b1, b2, ..., bm} if and
 * only if the cardinality of A is equal to the cardinality of B and for each
 * element, x, in A there exists a distinct element, y, in B for which x is
 * equivalent to y.
 *
 * <h4>Values</h4>
 * Any arbitrary, atomic value in the Ion Language can be denoted as the tuple,
 * (A, V), where A is an ordered list of annotations, and V is an Ion Primitive
 * Data or Ion Complex Data value. The list of annotations, A is an tuple of Ion
 * Symbols (a specific type of Ion Primitive).
 * </p>
 *
 * @author Almann Goo
 */
public final class Equivalence {

    /**
     * Marker for code that needs to be altered in order to support a public
     * comparison API to determine ordering of values, not just equality.
     */
    private static final boolean PUBLIC_COMPARISON_API = false;

    private static final boolean _debug_stop_on_false = true;

    private Equivalence() {
    }


    private static int compare(int i1, int i2)
    {
        if (i1 < i2) return -1;
        if (i1 > i2) return  1;
        return 0;
    }


    private static int compare(SymbolToken tok1, SymbolToken tok2)
    {
        String text1 = tok1.getText();
        String text2 = tok2.getText();
        if (text1 == null || text2 == null) {
            if (text1 != null) return  1;
            if (text2 != null) return -1;

            // otherwise v1 == v2 == null
            return compare(tok1.getSid(), tok2.getSid());
        }

        return text1.compareTo(text2);
    }


    /** Compare LOB content by stream--assuming non-null. */
    private static int lobContentCompare(final IonLob lob1, final IonLob lob2)
    {
        int in1 = lob1.byteSize();
        int in2 = lob2.byteSize();
        int pos = 0;
        int result = (in1- in2);

        if (result == 0) {
            final InputStream stream1 = lob1.newInputStream();
            final InputStream stream2 = lob2.newInputStream();

            // too bad Java doesn't do RAII with better syntax...
            try {
              try {
                try {
                  while (result == 0) {
                      in1 = stream1.read();
                      in2 = stream2.read();
                      if (in1 == -1 || in2 == -1) {
                        if (in1 != -1) result = 1;
                        if (in2 != -1) result = -1;
                        break;
                      }
                      result = (in1 - in2);
                      pos++;
                  }
                } finally {
                  stream1.close();
                }
              } finally {
                stream2.close();
              }
            } catch (final IOException e) {
                // XXX hopefully won't happen with LOB streams--if it does rethrow unchecked.
                //     this would violate Object.equals() would it not?
                throw new IonException(e);
            }
        }
        if (_debug_stop_on_false && result != 0) debugStopOnFalse();
        return result;
    }


    static private int _false_count = 0;
    static private int _false_target = -1;

    @SuppressWarnings("unused")
    static void debugStopOnFalse() {
        _false_count++;
        if (_debug_stop_on_false) {
            // you can put a break point here to catch failures
            return;
        }
        if (_false_count == _false_target) {
            return;
        }
        return;
    }

    /** Struct type item to aid in struct equality */
    private static class StructItem implements Comparable<StructItem> {
        public final String key;
        public final IonValue value;
        private final boolean strict;
        private int count;

        /**
         * Problematic with unknown field names.
         * See IonAssert for another use of this idiom.
         */
        public StructItem(final IonValue myValue, final boolean myStrict)
        {
            SymbolToken tok = myValue.getFieldNameSymbol();
            String k = tok.getText();
            if (k == null) {
                k = " -- UNKNOWN SYMBOL -- $" + tok.getSid(); // TODO ION-272
            }
            key = k;
            value = myValue;
            strict = myStrict;
            // we use -1 for searching
            count = -1;
        }

        public void incrementCount() {
            // increment count in a -1, means we are realizing this
            // as a true bag item
            count = count == -1 ? 1 : count + 1;
        }

        public int decrementCount() {
            count -= 1;
            return count;
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public boolean equals(final Object other) {
            // we can assume this is always a struct--internal usage dictates it
            final StructItem sOther = (StructItem) other;

            // we can also assume strict is the same--internal usage dictates it
            boolean answer = key.equals(sOther.key)
                && ionEqualsImpl(value, sOther.value, strict)
                // special case -1 means count matches always (for
                // searching)
                && (count == -1 || sOther.count == -1 || count == sOther.count);
            if (_debug_stop_on_false) {
                if (!answer) debugStopOnFalse();
            }
            return answer;
        }

        public int compareTo(StructItem other) {
            // we can also assume strict is the same--internal usage dictates it
            int answer = 0;

            answer = key.compareTo(other.key);
            if (answer == 0) {
                answer = ionCompareToImpl(value, other.value, strict);
                // special case -1 means count matches always (for
                // searching)
                if (answer == 0) {
                    if (count == 0 || other.count == -1) {
                        answer = 0;
                    }
                    else {
                        answer = count - other.count;
                    }
                }
            }
            if (_debug_stop_on_false) {
                if (answer != 0) debugStopOnFalse();
            }
            return answer;
        }
    }

    /**
     * We generate a map because we need to access the contents of the set
     * NB -- java.util.Set is missing a get() sort of API which is why this
     * doesn't work with Set.
     */
    private static final Map<StructItem, StructItem> createStructItems(final IonStruct source, final boolean strict) {
        final Map<StructItem, StructItem> values = new HashMap<StructItem, StructItem>();
        for (final IonValue val : source) {
            final StructItem item = new StructItem(val, strict);
            StructItem curr = values.get(item);
            if (curr == null) {
                // new item--put it in
                values.put(item, item);
                curr = item;
            }
            // at this point curr is an item in our bag,
            // we need to increment its count, if this is new it will
            // materialize it
            curr.incrementCount();
        }
        return values;
    }

    private static int ionCompareAnnotations(SymbolToken[] an1,
                                             SymbolToken[] an2)
    {
        int result = 0;

        int len = an1.length;
        if (len != an2.length) {
            result = len - an2.length;
        }
        else {
            for (int ii=0; (result == 0) && (ii < len); ii++) {
                result = compare(an1[ii], an2[ii]);
            }
        }

        return result;
    }

    private static final boolean ionEqualsImpl(final IonValue v1,
                                               final IonValue v2,
                                               final boolean strict)
    {
        return (ionCompareToImpl(v1, v2, strict) == 0);
    }

    private static int ionCompareToImpl(final IonValue v1,
                                        final IonValue v2,
                                        final boolean strict)
    {
        int result = 0;

        boolean      bo1, bo2;
        BigInteger   bi1, bi2;
        double       do1, do2;
        String       string1, string2;
        IonValue     stop_value;
        IonValue     next1, next2;


        if (_debug_stop_on_false) {
            stop_value = null;
        }

        if (v1 == null || v2 == null) {
            if (v1 != null) result = 1;
            if (v2 != null) result = -1;
            // otherwise v1 == v2 == null and result == 0
            if (_debug_stop_on_false) debugStopOnFalse();
            return result;
        }

        // check type
        IonType ty1 = v1.getType();
        IonType ty2 = v2.getType();
        result = ty1.compareTo(ty2);

        if (result == 0) {

            if (v1.isNullValue() || v2.isNullValue()) {
                 // null combination case--we already know they are
                 // the same type
                if (!v1.isNullValue()) result = 1;
                if (!v2.isNullValue()) result = -1;
                // othersize they're equal (and null values)

            }
            else {
                // value compare only if both are not null
                switch (ty1)
                {
                case NULL:
                    // never visited -- let it stay false
                    break;
                case BOOL:
                    bo1 = ((IonBool) v1).booleanValue();
                    bo2 = ((IonBool) v2).booleanValue();
                    if (bo1) {
                        result = bo2 ? 0 : 1;
                    }
                    else {
                        result = bo2 ? -1 : 0;
                    }
                    break;
                case INT:
                    bi1 = ((IonInt) v1).bigIntegerValue();
                    bi2 = ((IonInt) v2).bigIntegerValue();
                    result = bi1.compareTo(bi2);
                    break;
                case FLOAT:
                    do1 = ((IonFloat) v1).doubleValue();
                    do2 = ((IonFloat) v2).doubleValue();
                    result = Double.compare(do1, do2);
                    break;
                case DECIMAL:
                {
                    IonDecimal d1 = (IonDecimal) v1;
                    IonDecimal d2 = (IonDecimal) v2;

                    Decimal dec1 = d1.decimalValue();
                    Decimal dec2 = d2.decimalValue();

                    assert !PUBLIC_COMPARISON_API;
                    boolean eq = Decimal.equals(dec1, dec2);
                    result = (eq ? 0 : 1);
                    break;
                }
                case TIMESTAMP:
                {
                    Timestamp t1 = ((IonTimestamp) v1).timestampValue();
                    Timestamp t2 = ((IonTimestamp) v2).timestampValue();
                    // There's no "strict" before/after in Timestamp, so
                    // fall through to non-strict comparison.
                    if (strict) {
                        assert !PUBLIC_COMPARISON_API;
                        // Report inequality, but not proper ordering.
                        result = (t1.equals(t2) ? 0 : 1);
                    }
                    else {
                        result = t1.compareTo(t2);
                    }
                    break;
                }
                case STRING:
                    string1 = ((IonText) v1).stringValue();
                    string2 = ((IonText) v2).stringValue();
                    result = string1.compareTo(string2);
                    break;
                case SYMBOL:
                    result = compare(((IonSymbol) v1).symbolValue(),
                                     ((IonSymbol) v2).symbolValue());
                    break;
                case BLOB:
                case CLOB:
                    result = lobContentCompare((IonLob) v1, (IonLob) v2);
                    break;
                case STRUCT:
                    final IonStruct s1 = (IonStruct) v1;
                    final IonStruct s2 = (IonStruct) v2;
                    result = s1.size() - s2.size();
                    if (result == 0) {
                        // unfortunately struct's interface doesn't give us much
                        // options here
                        final Map<StructItem, StructItem> items1
                                = createStructItems(s1, strict);

                        for (IonValue val : s2) {
                            StructItem key = new StructItem(val, strict);
                            // this matches both the key and the value:
                            StructItem active = items1.get(key);
                            if (active == null) {
                                // nope we already have an inconsistency
                                result = -1;
                                if (_debug_stop_on_false) {
                                    stop_value = val;
                                }
                                break;
                            }
                            if (active.decrementCount() == 0) {
                                // we have nothing more left
                                // eliminate it from the source bag
                                items1.remove(key);
                            }
                        }
                        if (result == 0) {
                            result = items1.isEmpty() ? 0 : 1;
                        }
                    }
                    break;
                case LIST:
                case SEXP:
                case DATAGRAM:
                    final IonContainer c1 = (IonContainer) v1;
                    final IonContainer c2 = (IonContainer) v2;
                    result = c1.size() - c2.size();
                    if (result == 0) {
                        Iterator<IonValue> iter1 = c1.iterator();
                        Iterator<IonValue> iter2 = c2.iterator();
                        int ii = 0;
                        while (iter1.hasNext()) {
                            next1 = iter1.next();
                            next2 = iter2.next();
                            result = ionCompareToImpl(next1, next2, strict);
                            if (result != 0) {
                                break;
                            }
                            ii++;
                        }
                    }
                    break;
                }
            }
        }

        // if the values are otherwise equal, but the caller wants strict
        // comparison, then we check the annotations
        if (strict && (result == 0)) {
            // check tuple equality over the annotations
            // (which are strings)
            SymbolToken[] an1 = v1.getTypeAnnotationSymbols();
            SymbolToken[] an2 = v2.getTypeAnnotationSymbols();
            result = ionCompareAnnotations(an1, an2);
        }

        if (_debug_stop_on_false) {
            if (result != 0) {
                // just so we read stop value
                if (stop_value != null || true) {
                    debugStopOnFalse();
                }
            }
        }
        return result;
    }

    /**
     * Defines strict data equivalence over two Ion Values.
     *
     * @param v1
     *            The first Ion value to compare.
     * @param v2
     *            The second Ion value to compare.
     *
     * @return true if two Ion Values represent the same data.
     */
    public static final boolean ionEquals(final IonValue v1,
                                          final IonValue v2)
    {
        return ionEqualsImpl(v1, v2, true);
    }

    /**
     * Defines structural data equivalence over two Ion Values. That is,
     * equivalence without considering any annotations.
     *
     * @param v1
     *            The first Ion value to compare.
     * @param v2
     *            The second Ion value to compare.
     *
     * @return true if two Ion Values represent the same data without regard to
     *         annotations.
     */
    public static final boolean ionEqualsByContent(final IonValue v1,
                                                   final IonValue v2)
    {
        return ionEqualsImpl(v1, v2, false);
    }

}
