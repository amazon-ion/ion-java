// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion._private

/**
 * SpotBugs looks for an annotation called `SuppressFBWarnings` regardless of the package it is in.
 * This will allow us to disable some SpotBugs rules when we want e.g. to allow switch case fallthrough.
 *
 * We are implementing our own annotation so that we don't have to declare any dependency on another package just for this.
 */
annotation class SuppressFBWarnings(
    /**
     * The set of FindBugs/SpotBugs warnings that are to be suppressed in the annotated element.
     * The value can be a bug category, kind, or pattern.
     * For examples of some bug types, see https://spotbugs.readthedocs.io/en/stable/bugDescriptions.html
     */
    vararg val value: String = []
)
