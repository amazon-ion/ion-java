// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.apps.macroize;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonDatagram;
import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonType;
import com.amazon.ion.IonValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Specifies how a particular stream of Ion data should be written using Ion 1.1. This spec is read from an Ion file
 * that contains a struct with the following shape:
 * <pre>
 * {@code
 *     {
 *         macros: [(macro ...) ...] // The elements are Ion 1.1 TDL macro definitions
 *         textPatterns: [(verbatim | prefix | substring ...) ...] // The elements refer to {@link TextPattern} types
 *     }
 * }
 * </pre>
 * The textPattern elements may have the following shape:
 * <pre>
 * {@code
 *     (verbatim [string...]) // Each string in the list is a string to write as a symbol using make_string
 *     (prefix string [string...]) // The standalone string is the prefix; the optional list elements are potential suffixes.
 *     (substring string [string...]) // The standalone string is a target substring; the optional list elements are potential prefixes or suffixes.
 * }
 * </pre>
 * Note the following known limitations, which may be fixed in the future:
 * <ul>
 *     <li>Within macro definitions that expand to structs, variable names must match the field name,
 *         e.g., <code>{foo: (%foo)}</code></li>
 *     <li>The tool only attempts to match suggested macros to container values.</li>
 *     <li>Nested macro invocations are not yet supported.</li>
 * </ul>
 */
class MacroizeSpec {
    final List<MacroizeMacroMatcher> customMatchers = new ArrayList<>();
    final List<TextPattern> textPatterns = new ArrayList<>();

    /**
     * Reads the spec from the given reader. It is assumed that next() has not yet been called to position the reader
     * on the spec struct.
     * @param reader the reader.
     * @param context the encoding context.
     */
    void readSpec(IonReader reader, ManualEncodingContext context) {
        if (reader.next() != IonType.STRUCT) {
            throw new IonException("Expected struct.");
        }
        reader.stepIn();
        while (reader.next() != null) {
            if (reader.getType() != IonType.LIST) {
                throw new IonException("Expected list.");
            }
            switch (reader.getFieldName()) {
                case "macros":
                    readMacroMatchers(reader, context, customMatchers);
                    break;
                case "textPatterns":
                    readTextPatterns(reader, context, textPatterns);
                    break;
                default:
                    throw new IonException("Expected 'macros' or 'textPatterns'.");
            }
        }
    }

    private static void readMacroMatchers(IonReader reader, ManualEncodingContext symbolTable, List<MacroizeMacroMatcher> matchers) {
        reader.stepIn();
        while (reader.next() != null) {
            matchers.add(new MacroizeMacroMatcher(reader, symbolTable));
        }
        reader.stepOut();
    }

    private static void readTextPatterns(IonReader reader, ManualEncodingContext symbolTable, List<TextPattern> patterns) {
        reader.stepIn();
        while (reader.next() != null) {
            if (reader.getType() != IonType.SEXP) {
                throw new IonException("Expected s-exp.");
            }
            reader.stepIn();
            if (!IonType.isText(reader.next())) {
                throw new IonException("Expected pattern type name.");
            }
            switch (reader.stringValue()) {
                case "verbatim":
                    patterns.add(new VerbatimTextPattern(symbolTable, readStringList(reader)));
                    break;
                case "prefix":
                    if (!IonType.isText(reader.next())) {
                        throw new IonException("Expected prefix.");
                    }
                    patterns.add(new PrefixTextPattern(symbolTable, reader.stringValue(), readStringList(reader)));
                    break;
                case "substring":
                    if (!IonType.isText(reader.next())) {
                        throw new IonException("Expected substring.");
                    }
                    patterns.add(new SubstringTextPattern(symbolTable, reader.stringValue(), readStringList(reader)));
                    break;
                default:
                    throw new IonException("Expected 'stringAsSymbol', 'prefix', or 'contains'.");
            }
            reader.stepOut();
        }
        reader.stepOut();
    }

    private static List<String> readStringList(IonReader reader) {
        List<String> strings = new ArrayList<>();
        if (reader.next() == null) {
            return strings;
        }
        if (reader.getType() != IonType.LIST) {
            throw new IonException("Expected list of strings.");
        }
        reader.stepIn();
        while (reader.next() != null) {
            if (IonType.isText(reader.getType())) {
                strings.add(reader.stringValue());
            }
        }
        reader.stepOut();
        return strings;
    }

    private void recursiveMatch(IonContainer container, Map<String, Integer> matchCounter) {
        for (IonValue child : container) {
            for (MacroizeMacroMatcher customMatcher : customMatchers) {
                if (customMatcher.match(child)) {
                    matchCounter.compute(customMatcher.name(), (key, existingValue) -> {
                        if (existingValue == null) {
                            existingValue = 0;
                        }
                        return existingValue + 1;
                    });
                }
            }
            switch (child.getType()) {
                case STRUCT:
                case LIST:
                case SEXP:
                    recursiveMatch((IonContainer) child, matchCounter);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Match values from the given source against the macro matchers supplied by the spec. Logs the number of
     * occurrences of each macro match and assembles suggested signatures for each matcher with at least one match.
     * @param source the source data.
     * @param log the log to receive messages about occurrences.
     * @return a map from macro name to suggested signature for each name with at least one match.
     * @throws IOException if thrown when logging occurrences.
     */
    Map<String, SuggestedSignature> matchMacros(IonDatagram source, Appendable log) throws IOException {
        Map<String, Integer> customMacroMatches = new HashMap<>();
        Map<String, SuggestedSignature> suggestedSignatures = new HashMap<>();
        recursiveMatch(source, customMacroMatches);

        for (MacroizeMacroMatcher customMacroMatcher : customMatchers) {
            String matcherName = customMacroMatcher.name();
            Integer occurrences = customMacroMatches.get(matcherName);
            if (occurrences != null && occurrences > 0) {
                suggestedSignatures.put(matcherName, customMacroMatcher.getSignature());
                log.append(String.format("%n%n === %s (total occurrences: %d)%n", matcherName, occurrences));
            }
        }
        return suggestedSignatures;
    }
}
