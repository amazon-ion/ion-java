// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion;

import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonBinaryWriterBuilder_1_1;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

public class IonEncodingVersionTest {

    @Test
    public void vendsBinaryWriterBuilders() {
        IonBinaryWriterBuilder writerBuilder_1_0 = IonEncodingVersion.ION_1_0.binaryWriterBuilder();
        assertNotNull(writerBuilder_1_0);
        assertNotSame(writerBuilder_1_0, IonEncodingVersion.ION_1_0.binaryWriterBuilder());
        IonBinaryWriterBuilder_1_1 writerBuilder_1_1 = IonEncodingVersion.ION_1_1.binaryWriterBuilder();
        assertNotNull(writerBuilder_1_1);
        assertNotSame(writerBuilder_1_1, IonEncodingVersion.ION_1_1.binaryWriterBuilder());
    }
}
