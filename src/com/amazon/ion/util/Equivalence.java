package com.amazon.ion.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.amazon.ion.IonBool;
import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDecimal;
import com.amazon.ion.IonException;
import com.amazon.ion.IonFloat;
import com.amazon.ion.IonInt;
import com.amazon.ion.IonLob;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonText;
import com.amazon.ion.IonTimestamp;
import com.amazon.ion.IonValue;

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

    private Equivalence() {
    }

    /** Compare LOB content by stream--assuming non-null. */
    private static boolean lobContentCompare(final IonLob lob1, final IonLob lob2) {
        boolean byteEquals = lob1.byteSize() == lob2.byteSize();
        if (byteEquals) {
            final InputStream stream1 = lob1.newInputStream();
            final InputStream stream2 = lob2.newInputStream();

            // too bad Java doesn't do RAII with better syntax...
            try {
              try {
                try {
                  int in1 = -1;
                  int in2 = -1;
                  while (byteEquals
                      && (in1 = stream1.read()) != -1
                      && (in2 = stream2.read()) != -1) {
                      byteEquals = in1 == in2;
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
        return byteEquals;
    }

    /** Struct type item to aid in struct equality */
    private static class StructItem {
        public final String key;
        public final IonValue value;
        private final boolean strict;
        private int count;

        public StructItem(final String myKey,
                          final IonValue myValue,
                          final boolean myStrict) {
            assert myKey != null;
            key = myKey;
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
            return key.equals(sOther.key)
                && ionEqualsImpl(value, sOther.value, strict)
                // special case -1 means count matches always (for
                // searching)
                && (count == -1 || sOther.count == -1 || count == sOther.count);
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
            final StructItem item = new StructItem(val.getFieldName(), val, strict);
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

    private static final boolean ionEqualsImpl(final IonValue v1,
                                               final IonValue v2,
                                               final boolean strict) {
        boolean result = v1 == null ? v2 == null : v2 != null;

        if (!result) {
            // simple non-null/null combo case
            return false;
        } else if (v1 == null) {
            // simple both null case
            return true;
        }

        if (strict) {
            // check tuple equality over the annotations
            // (which are strings)
            result = Arrays.equals(v1.getTypeAnnotations(), v2.getTypeAnnotations());
        }

        // check type
        result = result && v1.getType() == v2.getType();
        
        if (result) {
            // now we assume false unless we can prove otherwise
            result = false;

            // value compare only if both are not null
            if (!v1.isNullValue() && !v2.isNullValue()) {
                switch (v1.getType())
                {
                case NULL:
                    // never visited -- let it stay false
                    break;
                case BOOL:
                    result = ((IonBool) v1).booleanValue()
                          == ((IonBool) v2).booleanValue();
                    break;
                case INT:
                    result = ((IonInt) v1).toBigInteger().equals(
                             ((IonInt) v2).toBigInteger());
                    break;
                case FLOAT:
                    result = Double.doubleToLongBits(((IonFloat) v1).doubleValue())
                          == Double.doubleToLongBits(((IonFloat) v2).doubleValue());
                    break;
                case DECIMAL:
                    result = ((IonDecimal) v1).toBigDecimal().equals(
                             ((IonDecimal) v2).toBigDecimal());
                    break;
                case TIMESTAMP:
                    final IonTimestamp t1 = (IonTimestamp) v1;
                    final Integer offset1 = t1.getLocalOffset();
                    final IonTimestamp t2 = (IonTimestamp) v2;
                    final Integer offset2 = t2.getLocalOffset();
                    result = t1.getMillis() == t2.getMillis()
                          && (offset1 == null
                                  ? offset2 == null
                                  : offset1.equals(offset2));
                    break;
                case STRING:
                case SYMBOL:
                    result = ((IonText) v1).stringValue().equals(
                             ((IonText) v2).stringValue());
                    break;
                case BLOB:
                case CLOB:
                    result = lobContentCompare((IonLob) v1, (IonLob) v2);
                    break;
                case STRUCT:
                    final IonStruct s1 = (IonStruct) v1;
                    final IonStruct s2 = (IonStruct) v2;
                    if (s1.size() == s2.size()) {
                        // unfortunately struct's interface doesn't give us much
                        // options here
                        final Map<StructItem, StructItem> items1
                                = createStructItems(s1, strict);
                        boolean inconsistent = false;
                        for (IonValue val : s2) {
                            StructItem key =
                                new StructItem(val.getFieldName(), val, strict);
                            StructItem active = items1.get(key);
                            if (active == null) {
                                // nope we already have an inconsistency
                                inconsistent = true;
                                break;
                            }
                            if (active.decrementCount() == 0) {
                                // we have nothing more left
                                // eliminate it from the source bag
                                items1.remove(key);
                            }
                        }
                        if (inconsistent) {
                            result = false;
                        } else {
                            result = items1.isEmpty();
                        }
                    }
                    break;
                case LIST:
                case SEXP:
                case DATAGRAM:
                    final IonContainer c1 = (IonContainer) v1;
                    final IonContainer c2 = (IonContainer) v2;
                    if (c1.size() == c2.size()) {
                        // assume true
                        result = true;
                        Iterator<IonValue> iter1 = c1.iterator();
                        Iterator<IonValue> iter2 = c2.iterator();
                        while (iter1.hasNext()) {
                            final IonValue next1 = iter1.next();
                            final IonValue next2 = iter2.next();

                            result = ionEqualsImpl(next1, next2, strict);
                            if (!result) {
                                break;
                            }
                        }
                    }
                    break;
                }
            } else {
                // null combination case--we already know they are
                // the same type
                result = v1.isNullValue() 
                      && v2.isNullValue();
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
    public static final boolean ionEquals(final IonValue v1, final IonValue v2) {
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
    public static final boolean ionEqualsByContent(final IonValue v1, final IonValue v2) {
        return ionEqualsImpl(v1, v2, false);
    }

}
