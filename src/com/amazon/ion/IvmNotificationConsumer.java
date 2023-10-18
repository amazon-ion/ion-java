// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ion;

@FunctionalInterface
public interface IvmNotificationConsumer {

    /**
     * Invoked when the cursor encounters an Ion version marker (IVM).
     *
     * @param majorVersion the major version indicated by the new IVM.
     * @param minorVersion the minor version indicated by the new IVM.
     */
    void ivmEncountered(int majorVersion, int minorVersion);
}
