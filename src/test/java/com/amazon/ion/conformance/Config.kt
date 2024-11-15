// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.conformance

import com.amazon.ion.system.IonReaderBuilder
import java.io.File

/** Top-level configuration for running the conformance tests */
data class Config(
    /** Controls whether debug printing should be turned on. */
    val debugEnabled: Boolean = true,
    /** If a NotImplementedError is encountered, should we fail the test or ignore it. */
    val failUnimplemented: Boolean = false,
    /** Use for a skip list, or for running only one or two tests. Return true to run the test. */
    val testFilter: (File, String) -> Boolean = { _, _ -> true },
    /** Named set of reader builders (i.e. different reader configurations) to use for all tests. */
    val readerBuilder: IonReaderBuilder,
) {
    fun newCaseBuilder(file: File) = ConformanceTestBuilder(this, file)
}
