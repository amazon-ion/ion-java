/*
 * Copyright 2013-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.ion.impl;

import static software.amazon.ion.junit.IonAssert.assertIonEquals;

import org.junit.Test;
import software.amazon.ion.IonContainer;
import software.amazon.ion.IonDatagram;
import software.amazon.ion.IonList;
import software.amazon.ion.IonSequence;
import software.amazon.ion.IonSexp;
import software.amazon.ion.IonStruct;
import software.amazon.ion.IonType;
import software.amazon.ion.IonWriter;
import software.amazon.ion.SystemSymbols;

/**
 * {@link OptimizedBinaryWriterTestCase} tests related to
 * the internal patching mechanism of binary writers.
 */
public class OptimizedBinaryWriterLengthPatchingTest
    extends OptimizedBinaryWriterTestCase
{
    /**
     * Variants of different container values as Ion text of different type
     * descriptor lengths (i.e., L in the type desc octet, not VarUInt Length).
     * Refer to Ion's binary format spec for more info.
     */
    protected enum ContainerTypeDescLengthVariant
    {
        LIST_L0         ("[]"),
        LIST_L1         ("[ 0 ]"),
        LIST_L2         ("[ 1 ]"),
        LIST_L13        ("[ 0, 1, 2, 3, 4, 5, 6 ]"),
        LIST_L14        ("[ 1, 2, 3, 4, 5, 6, 7, 8, 9 ]"),
        LIST_L15        ("null.list"),

        SEXP_L0         ("()"),
        SEXP_L1         ("( 0 )"),
        SEXP_L2         ("( 1 )"),
        SEXP_L13        ("( 0 1 2 3 4 5 6 )"),
        SEXP_L14        ("( 1 2 3 4 5 6 7 8 9 )"),
        SEXP_L15        ("null.sexp"),

        STRUCT_L0       ("{}"),
        STRUCT_L1       ("{ name: 1 }"), // NB: using system symbol
        STRUCT_L15      ("null.struct"),
        STRUCT_LEXISTS  ("{ name: 1, version: 2 }") // NB: using system symbols
        ;

        private String myText;

        ContainerTypeDescLengthVariant(String text)
        {
            this.myText = text;
        }

        // Not overriding toString() here as that'll mess with JUnit's reporting
        public String getText()
        {
            return myText;
        }
    }

    /**
     * Helper method for {@link #testOptimizedWriteValueBetweenContainers()}.
     * Copies children values from the reader to the writer.
     */
    private void copyContainerElements()
        throws Exception
    {
        ir.next();

        ir.stepIn();
        {
            while (ir.next() != null)
            {
                checkWriteValue(isStreamCopyOptimized());
            }
        }
        ir.stepOut();
    }

    /**
     * Test copy optimized writeValue when the IonReader is stepped-in.
     */
    @Test
    public void testOptimizedWriteValueBetweenContainers()
        throws Exception
    {
        String body =
            "{ fred_1: 123, fred_1: fred_1, fred_1: fred_1::null } " + // struct
            "[ 123, fred_1, fred_1::null ] " +                         // list
            "( 123 fred_1 fred_1::null ) " +                           // sexp
            "[[[ 123, fred_1, fred_1::null ]]]" +                      // deeply nested list
            "[ 123, fred_1, fred_1::null ]";                           // list

        byte[] source = encode(body);
        ir = makeReaderProxy(source);
        iw = makeWriter();

        // Prime the local symtab so it matches the source's symtab
        iw.writeSymbol("fred_1");

        //=== Struct ===
        copyContainerElements();

        //=== List ===
        copyContainerElements();

        //=== Sexp ===
        copyContainerElements();

        //=== deeply nested List ===
        ir.next();
        ir.stepIn();
        {
            ir.next();
            ir.stepIn();
            {
                copyContainerElements();
            }
            ir.stepOut();
        }
        ir.stepOut();

        //=== copy to a stepped-in-sexp writer ===
        iw.stepIn(IonType.SEXP);
        {
            copyContainerElements();
        }
        iw.stepOut();

        // Create the expected Ion data as text, which will be loaded into
        // a datagram later for equality check.

        StringBuilder sb = new StringBuilder();
        String scalars = "123 fred_1 fred_1::null "; // scalars in each container
        sb.append("fred_1 ");                   // from priming of local symtab
        sb.append(scalars);                     // from struct
        sb.append(scalars);                     // from list
        sb.append(scalars);                     // from sexp
        sb.append(scalars);                     // from deeply nested list
        sb.append("(" + scalars + ")");         // stepped-in-sexp writer

        String expectedIonText = sb.toString();

        IonDatagram expected = loader().load(expectedIonText);
        IonDatagram actual   = loader().load(outputByteArray());
        assertIonEquals(expected, actual);
    }

    private void writeTypeDescLengthVariants()
        throws Exception
    {
        for (ContainerTypeDescLengthVariant variant
            : ContainerTypeDescLengthVariant.values())
        {
            byte[] bytes = encode(variant.getText());

            ir = makeReaderProxy(bytes);
            ir.next();

            if (iw.isInStruct())
            {
                iw.setFieldName(SystemSymbols.NAME); // NB: system symbol
            }
            checkWriteValue(isStreamCopyOptimized());
        }
    }

    private void addTypeDescLengthVariants(IonContainer container)
        throws Exception
    {
        if (container instanceof IonSequence)
        {
            IonSequence seq = (IonSequence) container;
            for (ContainerTypeDescLengthVariant variant
                : ContainerTypeDescLengthVariant.values())
            {
                seq.add(oneValue(variant.getText()));
            }
        }
        else
        {
            IonStruct struct = (IonStruct) container;
            for (ContainerTypeDescLengthVariant variant
                : ContainerTypeDescLengthVariant.values())
            {
                struct.add(SystemSymbols.NAME, oneValue(variant.getText()));
            }
        }
    }

    /**
     * Helper method for {@link #testOptimizedWriteLengthPatching()}.
     */
    private void writeAndAddTypeDescLengthVariants(IonContainer container)
        throws Exception
    {
        writeTypeDescLengthVariants();
        addTypeDescLengthVariants(container);
    }

    /**
     * Tests that the internal implementation of binary {@link IonWriter}'s
     * patching mechanism is correct.
     *
     * @see IonWriterSystemBinary
     */
    @Test
    public void testOptimizedWriteLengthPatching()
        throws Exception
    {
        iw = makeWriter();

        // The expected datagram - as values are copied to the IonWriter, we
        // book-keep the values written so as to do an equality check at the end.
        IonDatagram expected = system().newDatagram();

        //=== top level ===
        writeAndAddTypeDescLengthVariants(expected);

        //=== nested list ===
        iw.stepIn(IonType.LIST);
        IonList expectedNestedList = expected.add().newEmptyList();
        {
            writeAndAddTypeDescLengthVariants(expectedNestedList);
        }
        iw.stepOut();

        //=== nested sexp ===
        iw.stepIn(IonType.SEXP);
        IonSexp expectedNestedSexp = expected.add().newEmptySexp();
        {
            writeAndAddTypeDescLengthVariants(expectedNestedSexp);
        }
        iw.stepOut();

        //=== nested struct ===
        iw.stepIn(IonType.STRUCT);
        IonStruct expectedNestedStruct = expected.add().newEmptyStruct();
        {
            writeAndAddTypeDescLengthVariants(expectedNestedStruct);
        }
        iw.stepOut();

        //=== deeply nested list ===
        iw.stepIn(IonType.LIST);
        IonList nestedList1 = expected.add().newEmptyList();
        {
            writeAndAddTypeDescLengthVariants(nestedList1);

            iw.stepIn(IonType.LIST);
            {
                IonList nestedList2 = nestedList1.add().newEmptyList();

                writeAndAddTypeDescLengthVariants(nestedList2);
            }
            iw.stepOut();
        }
        iw.stepOut();

        IonDatagram actual = loader().load(outputByteArray());

        assertIonEquals(expected, actual);
    }
}
