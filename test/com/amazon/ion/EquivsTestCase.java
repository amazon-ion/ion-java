// Copyright (c) 2013 Amazon.com, Inc.  All rights reserved.

package com.amazon.ion;

import static com.amazon.ion.IonType.DATAGRAM;

import com.amazon.ion.impl._Private_Utils;
import com.amazon.ion.junit.IonAssert;
import java.io.File;
import org.junit.Test;

public class EquivsTestCase
    extends IonTestCase
{
    /**
     * If an IonValue (inside a sequence) is annotated with this string, it
     * denotes that the IonValue is an IonString and its
     * {@link IonString#stringValue()} is to be interpretted as an IonDatagram.
     */
    private static final String EMBEDDED_DOCUMENTS_ANNOTATION =
        "embedded_documents";

    private static final String SCHEMA_CLASS_CAST_ERROR_MSG_HEADER =
        "Test files must follow the schema: top-level sequences with each " +
        "sequence consisting of at least two ";

    private static final String SCHEMA_CLASS_CAST_ERROR_MSG =
        SCHEMA_CLASS_CAST_ERROR_MSG_HEADER + "IonValues";

    private static final String SCHEMA_CLASS_CAST_ERROR_MSG_EMBEDDED_DOCUMENT =
        SCHEMA_CLASS_CAST_ERROR_MSG_HEADER + "IonStrings with embedded Ion documents";

    /**
     * The expected return values of equivalence checks.
     */
    private boolean myExpectedEquality;

    private File myTestFile;

    public void setTestFile(File file)
    {
        myTestFile = file;
    }

    public EquivsTestCase(boolean expectedEquality)
    {
        myExpectedEquality = expectedEquality;
    }

    /**
     * Perform equivalence checks on the test file encapsulated by the
     * {@code datagram}, ensuring that equality checks between test inputs in
     * the sequence(s) are as expected.
     * <p>
     * The property of transitivity of equalities is not assumed here. In the
     * case where there are more than two test inputs in a sequence, each test
     * input will be exhaustively compared against every other test input in
     * the same sequence, symmetrically.
     *
     * @param datagram
     *                  an IonDatagram encapsulating the sequences of test input
     * @param expectedEquality
     *                  expected output of the equality check
     */
    protected void runEquivalenceChecks(final IonDatagram datagram,
                                        boolean expectedEquality)
    {
        int datagramSize = datagram.size();

        assertTrue("Test file must have at least one sequence",
                   datagramSize > 0);

        for (int dgIndex = 0; dgIndex < datagramSize; dgIndex++)
        {
            IonSequence sequence = (IonSequence) datagram.get(dgIndex);

            boolean embeddedDocuments =
                sequence.hasTypeAnnotation(EMBEDDED_DOCUMENTS_ANNOTATION);

            int sequenceSize = sequence.size();
            assertTrue("Each sequence must have at least two values",
                       sequenceSize > 1);

            try
            {
                for (int i = 0; i < sequenceSize; i++)
                {
                    IonValue thisValue = sequence.get(i);
                    if (embeddedDocuments)
                    {
                        assertFalse("Embedded input cannot be null",
                                    thisValue.isNullValue());
                        IonString thisString = (IonString) thisValue;
                        thisValue = loader().load(thisString.stringValue());
                    }

                    for (int j = i + 1; j < sequenceSize; j++)
                    {
                        IonValue otherValue = sequence.get(j);
                        if (embeddedDocuments)
                        {
                            assertFalse("Embedded input cannot be null",
                                        otherValue.isNullValue());
                            IonString otherString = (IonString) otherValue;
                            otherValue = loader().load(otherString.stringValue());
                        }

                        try
                        {
                            checkEquivalence(thisValue, otherValue, expectedEquality);
                        }
                        catch (AssertionError e)
                        {
                            String message =
                                "Error comparing children " + i + " and " + j +
                                " of equivalence sequence " + dgIndex +
                                " (zero-based), expected: " + expectedEquality +
                                ", actual: " + !expectedEquality;
                            message += "\nthisValue: " + thisValue;
                            message += "\notherValue: " + otherValue;
                            fail(message);
                        }
                    }
                }
            }
            catch (ClassCastException e)
            {
                // ClassCastExceptions are throw (and caught here) when the
                // test file doesn't follow the agreed upon schema.
                if (embeddedDocuments)
                {
                    fail(SCHEMA_CLASS_CAST_ERROR_MSG_EMBEDDED_DOCUMENT);
                }
                fail(SCHEMA_CLASS_CAST_ERROR_MSG);
            }
        }
    }

    /**
     * Check strict equivalence or non-equivalence between two IonValues.
     * <p>
     * This also checks for the symmetric property of equalities,
     * i.e. X is equivalent to Y if and only if Y is equivalent to X.
     *
     * @param expectedEquality the result that {@link IonValue#equals(Object)}
     *          should return
     */
    protected void checkEquivalence(final IonValue left,
                                    final IonValue right,
                                    boolean expectedEquality)
    {
        if (expectedEquality)
        {
            IonAssert.assertIonEquals(left, right);
            IonAssert.assertIonEquals(right, left);

            // IonDatagram's hashCode() is unsupported
            if (left.getType()  != DATAGRAM &&
                right.getType() != DATAGRAM)
                assertEquals("Equal values have unequal hashes",
                             left.hashCode(), right.hashCode());
        }
        else
        {
            // Ensure the two values compared both ways return the same consistent
            // result, i.e. false. This is not related to the symmetric property
            // of equalities.
            assertFalse(left.equals(right));
            assertFalse(right.equals(left));
        }
    }

    @Test
    public void testEquivsOverFile()
    throws Exception
    {
        IonDatagram dg = loader().load(myTestFile);
        runEquivalenceChecks(dg, myExpectedEquality);
    }

    @Test
    public void testEquivsOverString()
    throws Exception
    {
        String ionText = _Private_Utils.utf8FileToString(myTestFile);
        IonDatagram dg = loader().load(ionText);
        runEquivalenceChecks(dg, myExpectedEquality);
    }

}
