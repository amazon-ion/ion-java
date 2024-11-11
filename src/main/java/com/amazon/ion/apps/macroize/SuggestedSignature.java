// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.apps.macroize;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Represents a simple suggested macro signature. TODO support + and * cardinalities.
 */
class SuggestedSignature {

    // Names of the required parameters (! cardinality), in the order they were added.
    private final Set<String> required = new LinkedHashSet<>();
    // Names of the optional parameters (? cardinality), in the order they were added.
    private final Set<String> optional = new LinkedHashSet<>();
    // Names of all parameters (required and optional), in the order they were added.
    private final Set<String> all = new LinkedHashSet<>();

    public void addRequired(String argument) {
        required.add(argument);
        all.add(argument);
    }

    public void addOptional(String argument) {
        optional.add(argument);
        all.add(argument);
    }

    public Set<String> allParameters() {
        return all;
    }

    /**
     * Gets the index of the target parameter in the sequence of all parameters. It is up to the caller to ensure
     * the target parameter exists.
     * @param targetParameter the target parameter
     * @return the index of the target parameter.
     */
    public int indexOf(String targetParameter) {
        int index = 0;
        for (String parameter : all) {
            if (targetParameter.equals(parameter)) {
                return index;
            }
            index++;
        }
        return index;
    }

    /**
     * @param candidate a set of parameter names to attempt to match to this signature.
     * @return true if the given parameters are compatible with this signature.
     */
    public boolean isCompatible(Set<String> candidate) {
        return candidate.containsAll(required) && all.containsAll(candidate);
    }
}
