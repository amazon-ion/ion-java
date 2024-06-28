// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.conformance

import com.amazon.ionelement.api.IonElement
import com.amazon.ionelement.api.location
import java.io.File

/** Exception for signalling invalid syntax in the conformance tests. */
class ConformanceTestInvalidSyntaxException(
    file: File,
    element: IonElement,
    description: String? = null,
    cause: Throwable? = null
) : Error(cause) {
    override val message: String = """
        Invalid conformance dsl syntax${ description?.let { "; $it" } ?: ""}
          - at file://${file.absolutePath}:${element.metas.location}
          - invalid clause was: $element
    """.trimIndent()
}
