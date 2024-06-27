// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.conformance

import com.amazon.ion.system.*
import java.io.File
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.TestFactory

object ConformanceTestRunner {
    val DEFAULT_READER_BUILDER_CONFIGURATIONS = mapOf(
        "default reader" to IonReaderBuilder.standard()
            .withCatalog(ION_CONFORMANCE_TEST_CATALOG),
        "incremental reader" to IonReaderBuilder.standard()
            .withCatalog(ION_CONFORMANCE_TEST_CATALOG)
            .withIncrementalReadingEnabled(true),
        // TODO: Other reader configurations
    )

    private val DEFAULT_SKIP_FILTER: (File, String) -> Boolean = { file, completeTestName ->
        // `completeTestName` is the complete name of the test — that is all the test descriptions in a particular
        // execution path, joined by ", ". (This is how it appears in the JUnit report.)
        when {
            // IonElement can't load $0. TODO: Use IonValue for `produces`, I guess.
            "$0" in completeTestName -> false
            // For some reason, $ion_symbol_table::null.struct is not handled as expected
            "IST structs are elided from app view" in completeTestName -> false
            // IonWriter is making it difficult to write invalid data
            "If no max_id, lack of exact-match must raise an error «then»" in completeTestName -> false
            // IonCatalog's "best choice" logic is not spec compliant
            // TODO—current test name has a typo. Update to correct spelling once ion-tests is fixed.
            "When max_id is valid, pad/truncade mismatched or absent SSTs" in completeTestName -> false
            // No support for reading `$ion_encoding` directives yet.
            "conformance/ion_encoding/" in file.absolutePath -> false
            file.endsWith("local_symtab_imports.ion") -> when {
                // FIXME: The writer seems to remove "imports" field if the value is `$ion_symbol_table`. This should be
                //        legal as per https://amazon-ion.github.io/ion-docs/docs/symbols.html#imports
                "Importing the current symbol table" in completeTestName -> false

                // WON'T FIX:

                // If you inspect the debug output, the serialized data does not include the repeated fields.
                // This implies that the writer is attempting to clean a user-supplied symbol table.
                "Repeated fields" in completeTestName -> false
                // For these tests, the writer is validating the max_id field, and failing before
                // we have a chance to test the reader.
                "If no max_id, lack of exact-match must raise an error" in completeTestName -> false
                "If max_id not non-negative int, lack of exact-match must raise an error" in completeTestName -> false
                else -> true
            }
            // Some of these are failing because
            //  - Ion Java doesn't support the Ion 1.1 system symbol table yet
            //  - The tokens `$ion_1_0` and `'$ion_1_0'` are never user values.
            // TODO: Add test names once they are added to this file
            file.endsWith("system_symbols.ion") -> false
            // $ion_literal not supported yet
            file.endsWith("ion_literal.ion") -> false
            else -> true
        }
    }

    private val CONFIG = Config(
        debugEnabled = true,
        failUnimplemented = false,
        readerBuilders = DEFAULT_READER_BUILDER_CONFIGURATIONS,
        testFilter = DEFAULT_SKIP_FILTER,
    )

    @TestFactory
    fun `Conformance Tests`(): Iterable<DynamicNode> {
        return ION_CONFORMANCE_DIR.walk()
            .filter { it.isFile && it.extension == "ion" }
            .map { file ->
                with(CONFIG.newCaseBuilder(file)) {
                    file.inputStream()
                        .let(ION::newReader)
                        .use { reader -> readAllTests(reader) }
                }
            }
            .asIterable()
    }
}
