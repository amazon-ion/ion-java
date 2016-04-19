/*
 * Copyright 2008-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.util;

import static software.amazon.ion.impl.PrivateIonConstants.UNKNOWN_SYMBOL_TEXT_PREFIX;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import software.amazon.ion.Decimal;
import software.amazon.ion.IonBool;
import software.amazon.ion.IonDecimal;
import software.amazon.ion.IonException;
import software.amazon.ion.IonFloat;
import software.amazon.ion.IonInt;
import software.amazon.ion.IonLob;
import software.amazon.ion.IonSequence;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonSymbol;
import software.amazon.ion.IonText;
import software.amazon.ion.IonTimestamp;
import software.amazon.ion.IonType;
import software.amazon.ion.IonValue;
import software.amazon.ion.SymbolToken;

/**
 * Provides equivalence comparisons between two {@link IonValue}s, following
 * the contract of {@link IonValue#equals(Object)}.
 *
 * <p>
 * Basic usage of this class is as follows:
 *
 *<pre>
 *    IonValue v1 = ...;
 *    IonValue v2 = ...;
 *    software.amazon.ion.util.Equivalence.ionEquals( v1, v2 );
 *</pre>
 *
 * More likely, a static import would make using this class easier.
 *
 *<pre>
 *    import static software.amazon.ion.util.Equivalence.ionEquals;
 *    ...
 *    boolean equivalent = ionEquals( v1, v2 );
 *</pre>
 *
 * </p>
 *
 * <p>
 * <h3>Ion Equivalence</h3>
 * In order to make Ion a useful model to describe data, we must first define
 * the notion of equivalence for all values representable in Ion. Equivalence
 * with respect to Ion values means that if two Ion values, X and Y, are
 * equivalent, they represent the same data and can be substituted for the other
 * without loss of information.
 *
 * This relationship is:
 * <ul>
 *   <li>
 *     symmetric: X is equivalent to Y if and only if Y is equivalent to X.
 *   </li>
 *   <li>
 *     transitive: if X is equivalent to Y and Y is equivalent to Z, then X is
 *     equivalent to Z.
 *   </li>
 *   <li>
 *     reflexive: X is equivalent to X.
 *   </li>
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
 */
public final class Equivalence {

    /**
     * TODO amznlabs/ion-java#26 Marker for code that needs to be altered in order to
     * support a public comparison API to determine ordering of values, not
     * just equality.
     */
    private static final boolean PUBLIC_COMPARISON_API = false;

    private Equivalence() {
    }


    private static int compareAnnotations(SymbolToken[] ann1,
                                          SymbolToken[] ann2)
    {
        int len = ann1.length;
        int result = len - ann2.length;

        if (result == 0) {
            for (int i=0; (result == 0) && (i < len); i++) {
                result = compareSymbolTokens(ann1[i], ann2[i]);
            }
        }

        return result;
    }


    private static int compareSymbolTokens(SymbolToken tok1,
                                           SymbolToken tok2)
    {
        String text1 = tok1.getText();
        String text2 = tok2.getText();
        if (text1 == null || text2 == null) {
            if (text1 != null) return  1;
            if (text2 != null) return -1;

            int sid1 = tok1.getSid();
            int sid2 = tok2.getSid();
            if (sid1 < sid2) return -1;
            if (sid1 > sid2) return  1;
            return 0;
        }

        return text1.compareTo(text2);
    }


    /**
     * Converts an IonStruct to a multi-set for use in IonStruct equality
     * checks. This method returns the multi-set as a {@code Map<Field, Field>}.
     * <p>
     * A multi-set supports order-independent equality, and may have duplicate
     * elements.
     * <p>
     * Take special note that {@link Set} is missing a {@code get()} API,
     * and cannot contain duplicate elements, hence we cannot use it.
     */
    private static final Map<Field, Field>
        convertToMultiSet(final IonStruct struct, final boolean strict) {

        final Map<Field, Field> structMultiSet =
            new HashMap<Field, Field>();

        for (final IonValue val : struct) {
            final Field item = new Field(val, strict);
            Field curr = structMultiSet.put(item, item);
            // curr will be non-null if the multi-set already contains the
            // name/value pair
            if (curr != null) {
                // Set the 'occurrences' of the Field that is contained in
                // the multi-set (i.e. item) to that of the previously mapped
                // Field (i.e. curr)
                item.occurrences = curr.occurrences;
            }
            // At this point, item will be an existing
            // name/value pair in the multi-set - increment its occurrence
            item.occurrences++;
        }

        return structMultiSet;
    }


    private static int compareStructs(final IonStruct s1,
                                      final IonStruct s2,
                                      boolean strict)
    {
        int result = s1.size() - s2.size();
        if (result == 0) {
            // We convert IonStruct s1 to a multi-set (which is a
            // Map<Field, Field>). Refer to convertToMultiSet()'s
            // documentation for more info
            final Map<Field, Field> s1MultiSet
                    = convertToMultiSet(s1, strict);

            // Iterates through each name/value pair in IonStruct s2 and
            // determine if it also occurs in s1MultiSet.
            // During each iteration:
            //          If it does, remove an occurrence from s1MultiSet
            //          If it doesn't, the two IonStructs aren't equal
            for (IonValue val : s2) {
                Field field = new Field(val, strict);

                // Find an occurrence of the name/value pair in s1MultiSet
                Field mappedValue = s1MultiSet.get(field);

                if (mappedValue == null || mappedValue.occurrences == 0) {
                    // No match in occurrences, the IonStructs aren't equal
                    return -1;
                }

                // Remove an occurrence by decrementing count instead of
                // explicitly calling Map.remove(), as Map.remove() is a slower
                // operation
                mappedValue.occurrences--;
            }
        }
        return result;
    }


    private static int compareSequences(final IonSequence s1,
                                        final IonSequence s2,
                                        boolean strict)
    {
        int result = s1.size() - s2.size();
        if (result == 0) {
            Iterator<IonValue> iter1 = s1.iterator();
            Iterator<IonValue> iter2 = s2.iterator();
            while (iter1.hasNext()) {
                result = ionCompareToImpl(iter1.next(),
                                          iter2.next(),
                                          strict);
                if (result != 0) {
                    break;
                }
            }
        }
        return result;
    }


    /** Compare LOB content by stream--assuming non-null. */
    private static int compareLobContents(final IonLob lob1, final IonLob lob2)
    {
        int in1 = lob1.byteSize();
        int in2 = lob2.byteSize();
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

        return result;
    }


    /**
     * Class that denotes a name/value pair in Structs.
     * <p>
     * Structs are unordered collections of name/value pairs. The names are
     * symbol tokens, and the values are unrestricted. Each name/value pair is
     * called a <em>field</em>.
     * <p>
     * The responsibilities of this class is to expose a
     * {@code Map<Field, Field>} as a multi-set. A Field instance holds the name
     * and value of a Struct field, and counts the number of times that
     * name/value pair occurs within the multi-set.
     * <p>
     * For example, an IonStruct:
     *<pre>
     *  {
     *    a : 123,
     *    a : 123
     *  }
     *</pre>
     * will be converted into a multi-set {@code Map<Field, Field>} with
     * a single {@code Field} -> {@code Field} with {@code occurrences} of 2.
     * <p>
     * Refer to
     * {@link Equivalence#convertToMultiSet(IonStruct, boolean)} and
     * {@link Field#equals(Object)} for more info.
     * <p>
     * NOTE: This class should only be instantiated for the sole purpose of
     * using it as either a <em>key</em> or <em>value</em> in a {@link Map}.
     */
    static class Field {
        private final String    name; // aka field name
        private final IonValue  value;
        private final boolean   strict;

        /**
         * Number of times that this specific field (with the same name
         * and value) occurs within the struct.
         */
        private int occurrences;

        Field(final IonValue value, final boolean strict)
        {
            SymbolToken tok = value.getFieldNameSymbol();
            String name = tok.getText();
            if (name == null) {
                // TODO amznlabs/ion-java#23 Problematic with unknown field names.
                name = UNKNOWN_SYMBOL_TEXT_PREFIX + tok.getSid();
            }
            this.name = name;
            this.value = value;
            this.strict = strict;

            // Occurrences of this name/value pair is 0 initially
            this.occurrences = 0;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
            // TODO amznlabs/ion-java#58 : implement hash code such that it respects
            // 'strict'. The prevously attempted fix is commented out below but
            // is not sufficient because value.hasCode will always include
            // type annotations in the hash computation. Type annotations
            // should not be part of the hash computation if strict=true.
//            result = (31 * result) + value.hashCode();
//            return result;
        }

        /**
         * This method is implicitly called by {@link Map#get(Object)} to
         * obtain a value to which {@code other} is mapped.
         */
        @Override
        public boolean equals(final Object other) {
            // We can assume other is always a Field and strict
            // is the same - internal usage dictates it.
            final Field sOther = (Field) other;

            return name.equals(sOther.name)
                && ionEqualsImpl(value, ((Field) other).value, strict);
        }
    }

    private static boolean ionEqualsImpl(final IonValue v1,
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

        if (v1 == null || v2 == null) {
            if (v1 != null) result = 1;
            if (v2 != null) result = -1;
            // otherwise v1 == v2 == null and result == 0
            return result;
        }

        // check type
        IonType ty1 = v1.getType();
        IonType ty2 = v2.getType();
        result = ty1.compareTo(ty2);

        if (result == 0) {
            boolean bo1 = v1.isNullValue();
            boolean bo2 = v2.isNullValue();

            if (bo1 || bo2) {
                 // null combination case--we already know they are
                 // the same type
                if (!bo1) result = 1;
                if (!bo2) result = -1;
                // othersize they're equal (and null values)
            }
            else {
                // value compare only if both are not null
                switch (ty1)
                {
                case NULL:
                    // never visited, precondition is that both are not null
                    break;
                case BOOL:
                    if (((IonBool) v1).booleanValue()) {
                        result = ((IonBool) v2).booleanValue() ? 0 : 1;
                    }
                    else {
                        result = ((IonBool) v2).booleanValue() ? -1 : 0;
                    }
                    break;
                case INT:
                    result = ((IonInt) v1).bigIntegerValue().compareTo(
                             ((IonInt) v2).bigIntegerValue());
                    break;
                case FLOAT:
                    result = Double.compare(((IonFloat) v1).doubleValue(),
                                            ((IonFloat) v2).doubleValue());
                    break;
                case DECIMAL:
                    assert !PUBLIC_COMPARISON_API; // TODO amznlabs/ion-java#26
                    result = Decimal.equals(((IonDecimal) v1).decimalValue(),
                                            ((IonDecimal) v2).decimalValue())
                                            ? 0 : 1;
                    break;
                case TIMESTAMP:
                    if (strict) {
                        assert !PUBLIC_COMPARISON_API; // TODO amznlabs/ion-java#26
                        result = (((IonTimestamp) v1).timestampValue().equals(
                                  ((IonTimestamp) v2).timestampValue())
                                  ? 0 : 1);
                    }
                    else {
                        // This is kind of lying here, the 'strict' boolean
                        // (if false) denotes ONLY that annotations are not
                        // check for equality. But what this is doing here is
                        // that it is also ignoring IonTimesamps' precision and
                        // local offset.
                        result = ((IonTimestamp) v1).timestampValue().compareTo(
                                 ((IonTimestamp) v2).timestampValue());
                    }
                    break;
                case STRING:
                    result = (((IonText) v1).stringValue()).compareTo(
                              ((IonText) v2).stringValue());
                    break;
                case SYMBOL:
                    result = compareSymbolTokens(((IonSymbol) v1).symbolValue(),
                                                 ((IonSymbol) v2).symbolValue());
                    break;
                case BLOB:
                case CLOB:
                    result = compareLobContents((IonLob) v1, (IonLob) v2);
                    break;
                case STRUCT:
                    assert !PUBLIC_COMPARISON_API; // TODO amznlabs/ion-java#26
                    result = compareStructs((IonStruct) v1,
                                            (IonStruct) v2,
                                            strict);
                    break;
                case LIST:
                case SEXP:
                case DATAGRAM:
                    result = compareSequences((IonSequence) v1,
                                              (IonSequence) v2,
                                              strict);
                    break;
                }
            }
        }

        // if the values are otherwise equal, but the caller wants strict
        // comparison, then we check the annotations
        if ((result == 0) && strict) {
            // check tuple equality over the annotations
            // (which are symbol tokens)
            result = compareAnnotations(v1.getTypeAnnotationSymbols(),
                                        v2.getTypeAnnotationSymbols());
        }

        return result;
    }

    /**
     * Checks for strict data equivalence over two Ion Values.
     *
     * @param v1
     *            The first Ion value to compare.
     * @param v2
     *            The second Ion value to compare.
     *
     * @return true if two Ion Values represent the same data.
     */
    public static boolean ionEquals(final IonValue v1,
                                    final IonValue v2)
    {
        return ionEqualsImpl(v1, v2, true);
    }

    /**
     * Checks for structural data equivalence over two Ion Values. That is,
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
    public static boolean ionEqualsByContent(final IonValue v1,
                                             final IonValue v2)
    {
        return ionEqualsImpl(v1, v2, false);
    }

}
