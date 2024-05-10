// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.system;

import com.amazon.ion.impl._Private_IonTextWriterBuilder_1_1;
import org.junit.Ignore;
import org.junit.Test;

public class IonTextWriterBuilder_1_1_Test extends IonTextWriterBuilderTestBase {
    @Override
    IonTextWriterBuilder standard() {
        return _Private_IonTextWriterBuilder_1_1.standard();
    }

    @Override
    String ivm() {
        return "$ion_1_1";
    }

    @Override
    @Test
    @Ignore
    public void testImports() {
        // TODO: skipped because IonManagedWriter_1_1 does not implement _Private_IonWriter.getSymbolTable.
    }

    @Override
    @Test
    @Ignore
    public void testCustomCatalog() {
        // TODO: skipped because IonManagedWriter_1_1 does not implement _Private_IonWriter.getCatalog.
    }

    @Override
    @Test
    @Ignore
    public void testIvmMinimization() {
        // TODO: skipped because the Ion 1.1 text writer does not yet support Ivm minimization.
    }
}
