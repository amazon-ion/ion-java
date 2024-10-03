// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.impl

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers
import org.junit.jupiter.api.Test

internal class LocalSymbolTableImportsTest {
    @Test
    fun `EMPTY#getImportedTables should be empty`() {
        assertThat(LocalSymbolTableImports.EMPTY.importedTables, Matchers.emptyArray())
    }
}
