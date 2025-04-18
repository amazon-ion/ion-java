// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.conformance

import com.amazon.ion.system.*
import java.io.File
import org.junit.jupiter.api.DynamicNode
import org.junit.jupiter.api.TestFactory

object DefaultReaderConformanceTests : ConformanceTestRunner(
    IonReaderBuilder.standard()
        .withCatalog(ION_CONFORMANCE_TEST_CATALOG)
)

object IncrementalReaderConformanceTests : ConformanceTestRunner(
    IonReaderBuilder.standard()
        .withCatalog(ION_CONFORMANCE_TEST_CATALOG)
        .withIncrementalReadingEnabled(true),
    additionalSkipFilter = { _, testName -> "Incomplete floats signal an error for unexpected EOF" in testName }
)

abstract class ConformanceTestRunner(
    readerBuilder: IonReaderBuilder,
    /** A predicate that returns `true` iff the test case should be skipped. */
    additionalSkipFilter: (File, String) -> Boolean = { _, _ -> false }
) {

    private val DEFAULT_SKIP_FILTER: (File, String) -> Boolean = { file, completeTestName ->
        // `completeTestName` is the complete name of the test — that is all the test descriptions in a particular
        // execution path, joined by " ". (This is how it appears in the JUnit report.)
        when {
            // IonElement can't load $0. TODO: Use IonValue for `produces`, I guess.
            "$0" in completeTestName -> false
            // For some reason, $ion_symbol_table::null.struct is not handled as expected
            "IST structs are elided from app view" in completeTestName -> false
            // IonWriter is making it difficult to write invalid data
            "If no max_id, lack of exact-match must raise an error «then»" in completeTestName -> false
            // IonCatalog's "best choice" logic is not spec compliant
            "When max_id is valid, pad/truncate mismatched or absent SSTs" in completeTestName -> false
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

            // FIXME: Contains test cases that are out of date, lack descriptions to have more specific exclusions
            "eexp/basic_system_macros.ion" in file.absolutePath -> false
            "eexp/arg_inlining.ion" in file.absolutePath -> false

            // FIXME:
            //   1. Test cases expect a zero-or-one-valued expression group to be valid for ? parameters, implementation disagrees
            //   2. One-to-many parameters are not raising an error for an empty expression group. This may need to be
            //      fixed in the macro evaluator and/or in the reader.
            //   3. All other failures for tagless type cases are due to "Encountered an unknown macro address: N" where
            //      N is the first byte of the macro argument (after any AEB and/or expression group prefixes).
            "eexp/binary/argument_encoding.ion" in file.absolutePath -> false

            // FIXME: All failing for reason #3 for argument_encoding.ion
            "eexp/binary/tagless_types.ion" in file.absolutePath -> false

            // FIXME: Fails because the encoding context isn't properly populated with the system module/macros
            "conformance/system_macros/" in file.absolutePath &&
                "in binary with a user macro address" in completeTestName -> false

            // FIXME: Fails due to unknown symbol text because IonReaderTextUserX doesn't have support for
            //        Ion 1.1 encoding modules or system symbol table
            "conformance/system_symbols.ion" in file.absolutePath &&
                "Ion 1.1 system symbol" in completeTestName -> false

            // FIXME: Timestamp should not allow an offset of +/-1440
            "the offset argument must be less than 1440" in completeTestName -> false
            "the offset argument must be greater than -1440" in completeTestName -> false

            // FIXME: Require these to be invoked at top level
            "set_symbols may not be invoked" in completeTestName -> false
            "add_symbols may not be invoked" in completeTestName -> false
            "set_macros may not be invoked" in completeTestName -> false
            "add_macros may not be invoked" in completeTestName -> false

            // FIXME: Ensure Ion 1.1 symbol tables are properly validated
            "add_symbols does not accept null.symbol" in completeTestName -> false
            "add_symbols does not accept null.string" in completeTestName -> false
            "add_symbols does not accept annotated arguments" in completeTestName -> false
            "set_symbols does not accept null.symbol" in completeTestName -> false
            "set_symbols does not accept null.string" in completeTestName -> false
            "set_symbols does not accept annotated arguments" in completeTestName -> false

            // FIXME: Add syntax checks in MacroCompiler
            "tdl/expression_groups.ion" in file.absolutePath -> false

            // FIXME: Implicit rest args don't always work
            "implicit rest args" in completeTestName -> false

            // FIXME: Ensure that the text reader throws if unexpected extra args are encountered
            "sum arguments may not be more than two integers" in completeTestName -> false
            "none signals an error when argument is" in completeTestName -> false

            // TODO: support continuable parsing of macro arguments
            "make_decimal can be invoked in binary using system macro address 6" in completeTestName -> false

            // TODO: Macro-shaped parameters not implemented yet
            "macro-shape" in completeTestName -> false

            // TODO: Not implemented yet
            "subnormal f16" in completeTestName -> false
            "conformance/system_macros/parse_ion.ion" in file.absolutePath -> false
            "tdl/for.ion" in file.absolutePath -> false

            // $ion_literal not supported yet
            file.endsWith("ion_literal.ion") -> false

            // WON'T FIX (probably): The top-level tokens `$ion_1_0` and `'$ion_1_0'` are never user values in IonJava
            "Ion 1.0 system symbol '\$ion_1_0'" in completeTestName -> false
            "Ion 1.1 system symbol '\$ion_1_0'" in completeTestName -> false
            "a symbol that looks like an Ion 1.0 IVM" in completeTestName -> false
            // FIXME: Even if we can't fix it for $ion_1_0, we can maybe fix it for $ion_1_1
            "a symbol that looks like an Ion 1.1 IVM" in completeTestName -> false

            else -> true
        }
    }

    private val CONFIG = Config(
        debugEnabled = true,
        failUnimplemented = false,
        readerBuilder = readerBuilder,
        testFilter = { file, name -> DEFAULT_SKIP_FILTER(file, name) && !additionalSkipFilter(file, name) },
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
