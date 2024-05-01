// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;

import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonBinaryWriterBuilder_1_1;
import com.amazon.ion.system._Private_IonBinaryWriterBuilder_1_1;

/**
 * Represents an Ion encoding version supported by this library.
 * <p>
 * Instances may be used to retrieve writer builders for the relevant Ion version. For example, to construct an
 * Ion 1.1 binary writer builder, use {@code ION_1_1.binaryWriterBuilder();}
 * </p>
 *
 * @param <BinaryWriterBuilder> the type of binary writer builder compatible with this version.
 */
// TODO add a parameter for the text writer builder type; add a "textWriterBuilder()" method.
public abstract class IonEncodingVersion<BinaryWriterBuilder> {

    /**
     * Ion 1.0, see the <a href="https://amazon-ion.github.io/ion-docs/docs/binary.html">binary</a> and
     * <a href="https://amazon-ion.github.io/ion-docs/docs/text.html">text</a> specification.
     */
    public static IonEncodingVersion<IonBinaryWriterBuilder> ION_1_0 = new IonEncodingVersion<IonBinaryWriterBuilder>(0) {

        @Override
        public IonBinaryWriterBuilder binaryWriterBuilder() {
            return IonBinaryWriterBuilder.standard();
        }
    };

    /**
     * Ion 1.1, TODO link to the finalized specification.
     */
    public static IonEncodingVersion<IonBinaryWriterBuilder_1_1> ION_1_1 = new IonEncodingVersion<IonBinaryWriterBuilder_1_1>(1) {

        @Override
        public IonBinaryWriterBuilder_1_1 binaryWriterBuilder() {
            return _Private_IonBinaryWriterBuilder_1_1.standard();
        }
    };

    private final int minorVersion;

    private IonEncodingVersion(int minorVersion) {
        this.minorVersion = minorVersion;
    }

    /**
     * Provides a new mutable binary writer builder for IonWriter instances that write this version of the Ion encoding.
     * @return a new mutable writer builder.
     */
    public abstract BinaryWriterBuilder binaryWriterBuilder();

    @Override
    public String toString() {
        return String.format("Ion 1.%d", minorVersion);
    }
}
