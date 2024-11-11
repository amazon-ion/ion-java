// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.apps.macroize;

import java.io.IOException;

@FunctionalInterface
interface ThrowingProcedure {
    void execute() throws IOException;
}
