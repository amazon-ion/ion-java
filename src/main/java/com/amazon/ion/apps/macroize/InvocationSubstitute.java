// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package com.amazon.ion.apps.macroize;

import com.amazon.ion.IonContainer;
import com.amazon.ion.IonSequence;
import com.amazon.ion.IonSexp;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Describes a macro invocation that should be substituted into a datagram in place of the literal value that is
 * currently there.
 * TODO this is needed because we currently don't have a way of describing a macro invocation in the DOM. If that
 *  changes, this may go away.
 */
class InvocationSubstitute {

    static final String INVOCATION_ANNOTATION = "$ion_invocation";
    static final String EMPTY_GROUP_ANNOTATION = "$ion_empty";
    private IonContainer parent;
    private int indexToReplace;
    private final String fieldNameToReplace;
    private final String shapeName;
    private final List<IonValue> parameters;
    private final SuggestedSignature signature;
    private final IonSystem system;

    /**
     * @param system the IonSystem that owns the parent container.
     * @param parent the parent container that holds the value to be replaced with an invocation.
     * @param indexToReplace the index in the parent of the value to be replaced.
     * @param fieldNameToReplace the field name of the value to be replaced, if in a struct.
     * @param shapeName the name of the macro to invoke.
     * @param signature the signature of the macro to invoke.
     */
    InvocationSubstitute(
        IonSystem system,
        IonContainer parent,
        int indexToReplace,
        String fieldNameToReplace,
        String shapeName,
        SuggestedSignature signature
    ) {
        this.system = system;
        this.parent = parent;
        this.indexToReplace = indexToReplace;
        this.fieldNameToReplace = fieldNameToReplace;
        this.shapeName = shapeName;
        this.parameters = extractArguments(parent, indexToReplace, fieldNameToReplace, signature);
        this.signature = signature;
    }

    /**
     * Retrieves the IonValue to be replaced with an invocation.
     * @param parent the parent container of the value to replace.
     * @param indexToReplace the index in the parent of the value to be replaced.
     * @param fieldNameToReplace the field name of the value to be replaced, if in a struct.
     * @return
     */
    private static IonValue select(IonContainer parent, int indexToReplace, String fieldNameToReplace) {
        IonValue target = null;
        if (fieldNameToReplace == null || !(parent instanceof IonStruct)) {
            Iterator<IonValue> children = parent.iterator();
            int index = 0;
            while (index <= indexToReplace) {
                index++;
                if (!children.hasNext()) {
                    return null;
                }
                target = children.next();
            }
        } else {
            target = ((IonStruct) parent).get(fieldNameToReplace);
        }
        return target;
    }

    /**
     * @return an IonSexp that is used to represent an empty expression group.
     */
    private IonSexp emptyExpressionGroup() {
        IonSexp empty = system.newEmptySexp();
        empty.addTypeAnnotation(EMPTY_GROUP_ANNOTATION);
        return empty;
    }

    /**
     * Extracts the values from the source data that must be passed into the invocation that will replace the current
     * value.
     * @param parent the parent container of the value to replace.
     * @param indexToReplace the index in the parent of the value to be replaced.
     * @param fieldNameToReplace the field name of the value to be replaced, if in a struct.
     * @param signature the signature of the invocation.
     * @return the list of arguments.
     */
    private List<IonValue> extractArguments(
        IonContainer parent,
        int indexToReplace,
        String fieldNameToReplace,
        SuggestedSignature signature
    ) {
        IonStruct targetStruct = (IonStruct) select(parent, indexToReplace, fieldNameToReplace);
        if (targetStruct == null) {
            throw new IllegalArgumentException("Failed to extract parameters for " + fieldNameToReplace);
        }
        List<IonValue> parameters = new ArrayList<>();
        for (String argument : signature.allParameters()) {
            IonValue parameter = targetStruct.get(argument);
            if (parameter == null) {
                // This is a missing optional
                parameters.add(emptyExpressionGroup());
            } else {
                parameters.add(parameter);
            }
        }
        // Remove all the optionals that occur contiguously at the end of the invocation.
        int tailOptionalCount = 0;
        for (int i = parameters.size() - 1; i >= 0; i--) {
            String[] annotations = parameters.get(i).getTypeAnnotations();
            if (annotations.length == 1 && annotations[0].equals(EMPTY_GROUP_ANNOTATION)) {
                tailOptionalCount++;
            } else {
                break;
            }
        }
        if (tailOptionalCount > 0) {
            parameters = parameters.subList(0, parameters.size() - tailOptionalCount);
        }
        return parameters;
    }

    /**
     * Substitutes the target value with an invocation.
     * @param nextDepthSubstitutes the substitutes at the next-greater depth. If the target values of those substitutes
     *                             were children of the value substituted in this method, then their parent and index
     *                             to replace must be updated to point at the new invocation.
     */
    public void substitute(List<InvocationSubstitute> nextDepthSubstitutes) {
        IonValue target = select(parent, indexToReplace, fieldNameToReplace);
        String fieldName = target == null ? null : target.getFieldName();
        IonSexp invocation = system.newEmptySexp();
        invocation.addTypeAnnotation(INVOCATION_ANNOTATION);
        invocation.add(system.newSymbol(shapeName));
        for (IonValue value : parameters) {
            value.removeFromContainer();
            invocation.add(value);
        }
        IonValue replaced;
        if (fieldName == null) {
            IonSequence parentSequence = ((IonSequence) parent);
            if (indexToReplace >= parentSequence.size()) {
                parentSequence.add(invocation);
                replaced = null;
            } else {
                replaced = parentSequence.set(indexToReplace, invocation);
            }
        } else {
            replaced = ((IonStruct) parent).get(fieldName);
            ((IonStruct) parent).put(fieldName, invocation);
        }

        if (nextDepthSubstitutes != null) {
            for (InvocationSubstitute nextDepthSubstitute : nextDepthSubstitutes) {
                if (nextDepthSubstitute.parent == replaced) {
                    nextDepthSubstitute.parent = invocation;
                    // The first index of an invocation starts at 1, since the macro name comes first.
                    nextDepthSubstitute.indexToReplace = signature.indexOf(nextDepthSubstitute.shapeName) + 1;
                }
            }
        }
    }
}
