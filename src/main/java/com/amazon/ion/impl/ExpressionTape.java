package com.amazon.ion.impl;

import com.amazon.ion.Decimal;
import com.amazon.ion.IntegerSize;
import com.amazon.ion.IonException;
import com.amazon.ion.IonType;
import com.amazon.ion.MacroAwareIonWriter;
import com.amazon.ion.SymbolToken;
import com.amazon.ion.Timestamp;
import com.amazon.ion.impl.bin.OpCodes;
import com.amazon.ion.impl.macro.Expression;
import com.amazon.ion.impl.macro.Macro;
import com.amazon.ion.impl.macro.SystemMacro;
import com.amazon.ion.impl.macro.TemplateMacro;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.amazon.ion.impl.ExpressionType.ANNOTATION_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.DATA_MODEL_CONTAINER_END_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.DATA_MODEL_CONTAINER_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.DATA_MODEL_SCALAR_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.EXPRESSION_GROUP_END_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.EXPRESSION_GROUP_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.E_EXPRESSION_END_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.E_EXPRESSION_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.NONE_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.TOMBSTONE_ORDINAL;
import static com.amazon.ion.impl.ExpressionType.VARIABLE_ORDINAL;

public class ExpressionTape { // TODO make internal

    public static class Element {
        Object context = null;
        Object value = null; // null for values that haven't yet been materialized
        byte type = -1;
        int start = -1;
        int end = -1;
        int containerStart = -1;
        int containerEnd = -1;
        String fieldName = null;
    }

    public static class Core {
        private Element[] elements;
        private int[][] expressionStarts;
        private int size = 0;
        private int[] numberOfExpressions;
        private int numberOfVariables = 0;
        private int[] variableStartsByInvocationArgumentOrdinal;
        private int[] variableOrdinalsByInvocationArgumentOrdinal;
        private int[][] variableStartsByVariableOrdinal;
        private int[] numberOfDuplicateUsagesByVariableOrdinal;
        private int[] eExpressionStarts;
        private int[] eExpressionEnds;
        private int numberOfEExpressions = 0;

        Core(int initialSize) {
            elements = new Element[initialSize];
            expressionStarts = new int[1][]; // TODO figure out why this breaks when the first dimension is increased, then increase it
            expressionStarts[0] = new int[16];
            numberOfExpressions = new int[1];
            variableStartsByInvocationArgumentOrdinal = new int[8];
            variableOrdinalsByInvocationArgumentOrdinal = new int[8];
            variableStartsByVariableOrdinal = new int[8][];
            numberOfDuplicateUsagesByVariableOrdinal = new int[8];
            eExpressionStarts = new int[8];
            eExpressionEnds = new int[8];
        }

        void grow() {
            elements = Arrays.copyOf(elements, elements.length * 2);
        }

        void growExpressionStartsForEExpression(int eExpressionIndex) {
            int[] expressionStartsForEExpression = expressionStarts[eExpressionIndex];
            expressionStarts[eExpressionIndex] = Arrays.copyOf(expressionStartsForEExpression, expressionStartsForEExpression.length * 2);
        }

        void ensureEExpressionIndexAvailable(int eExpressionIndex) {
            if (expressionStarts.length < eExpressionIndex) {
                expressionStarts = Arrays.copyOf(expressionStarts, expressionStarts.length + 1);
                expressionStarts[expressionStarts.length - 1] = new int[16];
                eExpressionStarts = Arrays.copyOf(eExpressionStarts, eExpressionStarts.length + 1);
                eExpressionEnds = Arrays.copyOf(eExpressionEnds, eExpressionEnds.length + 1);
                numberOfExpressions = Arrays.copyOf(numberOfExpressions, numberOfExpressions.length + 1);
            }
        }

        void setNextExpression(int eExpressionIndex, int index) {
            int numberOfExpressionsInEExpression = numberOfExpressions[eExpressionIndex]++;
            if (expressionStarts[eExpressionIndex].length <= numberOfExpressionsInEExpression) {
                growExpressionStartsForEExpression(eExpressionIndex);
            }
            expressionStarts[eExpressionIndex][numberOfExpressionsInEExpression] = index;
        }

        void setNextVariable(int index, int variableOrdinal) {
            if (variableStartsByInvocationArgumentOrdinal.length <= numberOfVariables) {
                variableStartsByInvocationArgumentOrdinal = Arrays.copyOf(variableStartsByInvocationArgumentOrdinal, variableStartsByInvocationArgumentOrdinal.length * 2);
                variableOrdinalsByInvocationArgumentOrdinal = Arrays.copyOf(variableOrdinalsByInvocationArgumentOrdinal, variableOrdinalsByInvocationArgumentOrdinal.length * 2);
            }
            /*
            int variableOrdinal = (int) elements[index].context;
            if (variableStartsByVariableOrdinal.length <= variableOrdinal) {
                variableStartsByVariableOrdinal = Arrays.copyOf(variableStartsByVariableOrdinal, variableStartsByVariableOrdinal.length * 2);
                variableStartsByVariableOrdinal[variableOrdinal] = new int[] { index };
                numberOfDuplicateUsagesByVariableOrdinal = Arrays.copyOf(numberOfDuplicateUsagesByVariableOrdinal, numberOfDuplicateUsagesByVariableOrdinal.length * 2);
            } else {
                int[] variableStartsForVariableOrdinal = variableStartsByVariableOrdinal[variableOrdinal];
                if (variableStartsForVariableOrdinal == null) {
                    variableStartsByVariableOrdinal[variableOrdinal] = new int[] { index };
                    numberOfDuplicateUsagesByVariableOrdinal[variableOrdinal] = 0;
                } else {
                    int numberOfDuplicatesOfThisVariable = numberOfDuplicateUsagesByVariableOrdinal[variableOrdinal];
                    if (variableStartsForVariableOrdinal.length - 1 <= numberOfDuplicatesOfThisVariable) {
                        variableStartsByVariableOrdinal[variableOrdinal] = Arrays.copyOf(variableStartsForVariableOrdinal, variableStartsForVariableOrdinal.length + 1);
                        numberOfDuplicateUsagesByVariableOrdinal[variableOrdinal] = ++numberOfDuplicatesOfThisVariable;
                    }
                    variableStartsByVariableOrdinal[variableOrdinal][numberOfDuplicatesOfThisVariable] = index;
                }
            }

             */
            variableOrdinalsByInvocationArgumentOrdinal[numberOfVariables] = variableOrdinal;
            variableStartsByInvocationArgumentOrdinal[numberOfVariables++] = index;
        }

        void cacheVariableLocationByOrdinal(int variableStartIndex, int variableOrdinal) {
            //int variableOrdinal = (int) elements[index].context;
            if (variableStartsByVariableOrdinal.length <= variableOrdinal) {
                while (variableStartsByVariableOrdinal.length <= variableOrdinal) { // Note: the while is required because the ordinals may be added out of order. Therefore, more than one growth may be required.
                    variableStartsByVariableOrdinal = Arrays.copyOf(variableStartsByVariableOrdinal, variableStartsByVariableOrdinal.length * 2);
                    variableStartsByVariableOrdinal[variableOrdinal] = new int[]{variableStartIndex};
                    numberOfDuplicateUsagesByVariableOrdinal = Arrays.copyOf(numberOfDuplicateUsagesByVariableOrdinal, numberOfDuplicateUsagesByVariableOrdinal.length * 2);
                }
            } else {
                int[] variableStartsForVariableOrdinal = variableStartsByVariableOrdinal[variableOrdinal];
                if (variableStartsForVariableOrdinal == null) {
                    variableStartsByVariableOrdinal[variableOrdinal] = new int[] { variableStartIndex };
                    numberOfDuplicateUsagesByVariableOrdinal[variableOrdinal] = 0;
                } else {
                    int numberOfDuplicatesOfThisVariable = numberOfDuplicateUsagesByVariableOrdinal[variableOrdinal];
                    if (variableStartsForVariableOrdinal.length - 1 <= numberOfDuplicatesOfThisVariable) {
                        variableStartsByVariableOrdinal[variableOrdinal] = Arrays.copyOf(variableStartsForVariableOrdinal, variableStartsForVariableOrdinal.length + 1);
                        numberOfDuplicateUsagesByVariableOrdinal[variableOrdinal] = ++numberOfDuplicatesOfThisVariable;
                    }
                    variableStartsByVariableOrdinal[variableOrdinal][numberOfDuplicatesOfThisVariable] = variableStartIndex;
                }
            }
            //numberOfVariables++;
        }

        int findEndOfExpression(int startIndex) {
            int relativeDepth = 0;
            int i = startIndex;
            while (i < size) {
                if (elements[i] == null) { // TODO this might be wrong/unnecessary, and check i vs i + 1
                    return i + 1;
                }
                switch (elements[i].type) {
                    case ANNOTATION_ORDINAL:
                    case DATA_MODEL_SCALAR_ORDINAL:
                    case VARIABLE_ORDINAL:
                    case NONE_ORDINAL:
                        if (relativeDepth == 0) {
                            /*
                            if (i > startIndex) {
                                break loop;
                            }

                             */
                            return i + 1;
                        }
                        break;
                    case E_EXPRESSION_ORDINAL:
                    case EXPRESSION_GROUP_ORDINAL:
                        relativeDepth++;
                        break;
                    case DATA_MODEL_CONTAINER_ORDINAL:
                        /*
                        if (relativeDepth == 0) {
                            if (i > startIndex) {
                                break loop;
                            }
                        }

                         */
                        i = elements[i].containerEnd;
                        if (relativeDepth == 0) {
                            return i + 1;
                        }
                        //relativeDepth++; // TODO check
                        break;
                    case DATA_MODEL_CONTAINER_END_ORDINAL: // TODO how can this happen?
                        break;
                    case EXPRESSION_GROUP_END_ORDINAL:
                    case E_EXPRESSION_END_ORDINAL:
                        if (--relativeDepth <= 0) {
                            //break loop;
                            return i + 1;
                        }
                        break;
                    case TOMBSTONE_ORDINAL:
                        i = elements[i].containerEnd;
                        continue;
                    default:
                        throw new IllegalStateException();
                }
                i++;
            }
            return i;
        }

        public void recalculateEExpressionEnd(int eExpressionIndex, int startIndex) {
            eExpressionStarts[eExpressionIndex] = startIndex;
            eExpressionEnds[eExpressionIndex] = findEndOfExpression(startIndex);
        }

        public void truncateEExpressionCaches(int truncatedLength, int currentLength) {
            this.numberOfEExpressions = truncatedLength;
            for (int i = truncatedLength; i < currentLength; i++) {
                numberOfExpressions[i] = 0;
            }
        }

        public int copyToVariable(int startIndex, int variableIndex, ExpressionTape other) {
            int endIndex = variableStartsByInvocationArgumentOrdinal[variableIndex];
            other.copyFromRange(this, startIndex, endIndex);
            return endIndex + 1;
        }

        public String fieldNameForVariable(int variableIndex) {
            return elements[variableStartsByInvocationArgumentOrdinal[variableIndex]].fieldName;
        }

        public int getNumberOfVariables() {
            return numberOfVariables;
        }

        public boolean areVariablesOrdered() {
            int previousVariableOrdinal = -1;
            for (int i = 0; i < numberOfVariables; i++) {
                int currentVariableOrdinal = (int) elements[variableStartsByInvocationArgumentOrdinal[i]].context;
                if (currentVariableOrdinal < previousVariableOrdinal) {
                    return false;
                }
                previousVariableOrdinal = currentVariableOrdinal;
            }
            return true;
        }

        /**
         * Given a variable index, which refers to the order that variables are used in this expression tape, returns
         * the ordinal of that variable, i.e. the order in which that variable occurs in the invocation.
         * For example, the IfNone system macro may have the tape (VARIABLE(0) VARIABLE(1) VARIABLE(0)). The variable
         * indices for that tape are (0 1 2) and the variable ordinals are (0 1 0).
         * @param invocationArgumentOrdinal the ordinal of an invocation argument, which the caller ensures is less
         *                                  than `numberOfVariables`.
         * @return the ordinal of the variable at the given index.
         */
        public int getVariableOrdinal(int invocationArgumentOrdinal) {
            return variableOrdinalsByInvocationArgumentOrdinal[invocationArgumentOrdinal];
        }

        public int[] getVariableStartIndices(int variableOrdinal) {
            return variableStartsByVariableOrdinal[variableOrdinal];
        }

        public int getNumberOfDuplicateUsages(int variableOrdinal) {
            return numberOfDuplicateUsagesByVariableOrdinal[variableOrdinal];
        }

        public int getExpressionStartIndex(int eExpressionIndex, int expressionOrdinal) {
            return expressionStarts[eExpressionIndex][expressionOrdinal];
        }

        public int findEndOfTombstoneSequence(int index) {
            if (index >= size) {
                return size;
            }
            if (elements[index] == null) {
                // TODO when does this happen? should it ever happen?
                return index;
            }
            while (elements[index].type == TOMBSTONE_ORDINAL) {
                index = elements[index].containerEnd;
            }
            return index;
        }

        public void setTombstoneAt(int startIndex, int endIndex) {
            Element element = elements[startIndex];
            if (element == null) {
                // TODO when does this happen? should it ever happen?
                element = new Element();
                elements[startIndex] = element;
            }
            element.type = TOMBSTONE_ORDINAL;
            //element.context = null;
            //element.value = null;
            //element.end = -1;
            //element.start = -1;
            //element.fieldName = null; // TODO tried removing, changed nothing in targeted test. Check.
            //element.containerStart = -1;
            element.containerEnd = endIndex;
        }

        public int getEExpressionStartIndex(int eExpressionIndex) {
            return eExpressionStarts[eExpressionIndex];
        }

        public int getEExpressionEndIndex(int eExpressionIndex) {
            return eExpressionEnds[eExpressionIndex];
        }

        public int getNumberOfEExpressions() {
            return numberOfEExpressions;
        }

        public int findNextTombstone(int startIndex) {
            int endIndex = startIndex + 1;
            while (elements[endIndex].type != TOMBSTONE_ORDINAL) {
                endIndex++;
            }
            return endIndex;
        }

        public void extendTombstoneFrom(int startIndex) {
            int endIndex = findNextTombstone(startIndex);
            Element startElement = elements[startIndex];
            startElement.type = TOMBSTONE_ORDINAL;
            // Extend the tombstone sequence to the end of the tombstone sequence beginning at 'endIndex'.
            startElement.containerEnd = elements[endIndex].containerEnd;
        }

        public Element elementAt(int index) {
            return elements[index];
        }

        public int size() {
            return size;
        }
    }

    public static final ExpressionTape EMPTY = new ExpressionTape(null, new Core(0));

    private Core core;
    private final IonReaderContinuableCoreBinary reader; // If null, then the values are materialized
    private int i = 0;
    private int iNext = 0;
    private int depth = 0;
    private int[] eExpressionActiveAtDepth = new int[8];
    private int currentDataModelContainerStart = 0;
    private int insertionLimit = Integer.MAX_VALUE;
    private boolean lastAddOverflowed = false;

    public ExpressionTape(IonReaderContinuableCoreBinary reader, int initialSize) {
        this.reader = reader;
        core = new Core(initialSize);
        Arrays.fill(eExpressionActiveAtDepth, -1);
    }

    public ExpressionTape(IonReaderContinuableCoreBinary reader, Core core) {
        this.reader = reader;
        this.core = core;
        Arrays.fill(eExpressionActiveAtDepth, -1);
    }

    public boolean checkAndClearOverflow() {
        if (lastAddOverflowed) {
            lastAddOverflowed = false;
            return true;
        }
        return false;
    }

    public ExpressionTape blankCopy() {
        return new ExpressionTape(reader, core.size);
    }

    public Core core() {
        return core;
    }

    public void reset(Core core) {
        this.core = core;
        rewindTo(0);
        insertionLimit = Integer.MAX_VALUE;
    }

    /**
     * Sets an insertion limit. If a call to `add` would add an element at the limit, the tape will be shifted right
     * to make room
     * @param limit the limit, or -1 to clear the limit.
     */
    public void setInsertionLimit(int limit) {
        insertionLimit = limit;
    }

    public void clearInsertionLimit() {
        insertionLimit = Integer.MAX_VALUE;
    }

    private void increaseDepth(boolean isEExpression) {
        depth++;
        if (depth >= eExpressionActiveAtDepth.length) {
            eExpressionActiveAtDepth = Arrays.copyOf(eExpressionActiveAtDepth, eExpressionActiveAtDepth.length * 2);
        }
        eExpressionActiveAtDepth[depth] = isEExpression ? core.numberOfEExpressions - 1 : -1;
    }

    private void decreaseDepth() {
        depth--;
        core.elements[i].containerStart = currentDataModelContainerStart;
    }

    private void setChildExpressionIndex() {
        if (eExpressionActiveAtDepth[depth] >= 0) {
            core.setNextExpression(eExpressionActiveAtDepth[depth], i);
        }
    }

    private void setExpressionStart(byte type) {
        switch (type) {
            case E_EXPRESSION_ORDINAL:
                setChildExpressionIndex();
                core.eExpressionStarts[core.numberOfEExpressions] = i;
                core.elements[i].end = core.numberOfEExpressions++; // TODO put this in containerEnd instead, then use start/end for e-expression start/end. Then get rid of the separate arrays for tracking e-expression start/end by index.
                core.ensureEExpressionIndexAvailable(core.numberOfEExpressions);
                core.elements[i].containerStart = currentDataModelContainerStart;
                increaseDepth(true);
                break;
            case DATA_MODEL_CONTAINER_ORDINAL:
                setChildExpressionIndex();
                core.elements[i].containerStart = currentDataModelContainerStart;
                currentDataModelContainerStart = i;
                increaseDepth(false);
                break;
            case EXPRESSION_GROUP_ORDINAL:
                setChildExpressionIndex();
                core.elements[i].containerStart = currentDataModelContainerStart;
                increaseDepth(false);
                break;
            case DATA_MODEL_SCALAR_ORDINAL:
            case ANNOTATION_ORDINAL:
            case NONE_ORDINAL:
                setChildExpressionIndex();
                core.elements[i].containerStart = currentDataModelContainerStart;
                break;
            case VARIABLE_ORDINAL:
                setChildExpressionIndex();
                core.setNextVariable(i, (int) core.elements[i].context);
                core.elements[i].containerStart = currentDataModelContainerStart;
                break;
            case E_EXPRESSION_END_ORDINAL:
                decreaseDepth();
                core.eExpressionEnds[eExpressionActiveAtDepth[depth + 1]] = i;
                break;
            case DATA_MODEL_CONTAINER_END_ORDINAL:
                decreaseDepth();
                core.elements[currentDataModelContainerStart].containerEnd = i;
                currentDataModelContainerStart = core.elements[currentDataModelContainerStart].containerStart;
                break;
            case EXPRESSION_GROUP_END_ORDINAL:
                decreaseDepth();
                break;
            default: break;
        }
    }

    public void markVariableStart(int variableOrdinal) {
        // TODO these and other marked tape indices will be incorrect after shift occurs; need to be recalculated.
        core.setNextVariable(i, variableOrdinal);
        core.cacheVariableLocationByOrdinal(i, variableOrdinal);
    }

    private void shiftRightToMakeRoom(int minimumSpaceRequired) {
        // TODO this is really expensive. May be possible to optimize if necessary. For example, the first time this
        //  happens on an invocation, could start writing overflowing parameters to scratch space, marking start/end
        //  indices and the total shortage. At the end of the invocation, can allocate a new tape once and zip together
        //  the existing tape and the scratch tape in order. That way there's a maximum of one new allocation per
        //  invocation. This also cuts down on over-allocation because the exact amount of extra space is known.
        // TODO 16 is arbitrary. Is it possible to choose a better value?
        Core newCore = new Core(core.size + Math.max(minimumSpaceRequired, 16)); // TODO larger size might not be required
        ExpressionTape newTape = new ExpressionTape(reader, newCore);
        //int savedInsertionLimit = insertionLimit;
        insertionLimit = Integer.MAX_VALUE;
        int currentIndex = i;
        rewindTo(0); // TODO check, didn't appear to do anything
        newTape.copyFromRange(core, 0, currentIndex); // TODO this could just be array copies up to the index
        for (int tombstoneIndex = 0; tombstoneIndex < 16; tombstoneIndex++) {
            newTape.add(null, TOMBSTONE_ORDINAL, null, null);
            newCore.elements[newTape.i].containerEnd = 16 - tombstoneIndex;
        }
        newTape.copyFromRange(core, currentIndex, core.size);
        core = newCore;
    }

    void add(Object context, byte type, int start, int end, String fieldName) {
        if (i >= insertionLimit) {
            //shiftRightToMakeRoom(i - insertionLimit);
            lastAddOverflowed = true;
            return;
        }
        if (i >= core.elements.length) {
            core.grow();
        }
        Element element = core.elements[i];
        if (element == null) {
            element = new Element();
            core.elements[i] = element;
        }
        element.context = context;
        element.type = type;
        element.value = null;
        element.start = start;
        element.end = end;
        element.fieldName = fieldName;
        setExpressionStart(type);
        i++;
        if (i > core.size) { // Do not increase size if this was an in-place add.
            core.size++;
        }
        /*
        if (i >= core.size) {
            setExpressionStart(type);
            i++;
            core.size++;
        } else { // Do not increase size if this was an in-place add.
            i++;
        }
         */
    }

    private void inlineExpression(Core source, int sourceStart, int sourceEnd, Core arguments, int argumentsStart, String fieldName) {
        int startIndex = core.size;
        for (int i = sourceStart; i < sourceEnd; i++) {
            Element sourceElement = source.elements[i];
            if (sourceElement.type == ExpressionType.VARIABLE_ORDINAL) {
                if (arguments == null) {
                    // This is a top-level template body. There is no invocation yet, so there is nothing to substitute.
                    copyFrom(source, i);
                    continue;
                }
                int variableIndex = (int) sourceElement.context;
                int eExpressionIndex = arguments.elements[argumentsStart].end;
                if (variableIndex >= arguments.numberOfExpressions[eExpressionIndex]) {
                    // This argument is elided.
                    Object previousContext = core.elements[core.size - 1].context;
                    if (previousContext == SystemMacro.Default || previousContext == SystemMacro.IfNone) {
                        // Elide the invocation, substitute the second argument, then skip to the end of the invocation.
                        core.size--;
                        this.i--;
                        int expressionEnd = source.findEndOfExpression(++i);
                        // Note: keep the existing field name.
                        inlineExpression(source, i, expressionEnd, null, -1, fieldName());
                        i = sourceEnd;
                    } else {
                        // TODO does this ever occur in a position where it could simply be elided?
                        add(null, NONE_ORDINAL, null, sourceElement.fieldName);
                    }
                } else {
                    int expressionStart = arguments.expressionStarts[eExpressionIndex][variableIndex];
                    int expressionEnd = arguments.findEndOfExpression(expressionStart);
                    inlineExpression(arguments, expressionStart, expressionEnd, null, -1, sourceElement.fieldName);
                }
            } else if (sourceElement.type == ExpressionType.E_EXPRESSION_ORDINAL) {
                // Recursive inlining
                // TODO have a limit on depth / size
                Macro macro = (Macro) sourceElement.context;
                Core expressionSource = null;
                if (macro instanceof TemplateMacro) {
                    expressionSource = macro.getBodyTape();
                } else if (macro instanceof SystemMacro) {
                    SystemMacro systemMacro = (SystemMacro) macro;
                    // Some system macros cannot be inlined because they have special behavior that is not captured
                    // by a template body.
                    if (systemMacro.getBody() != null) {
                        expressionSource = systemMacro.getBodyTape();
                    }
                }
                if (expressionSource != null) {
                    inlineExpression(expressionSource, 0, expressionSource.size, source, i, sourceElement.fieldName);
                    i = source.findEndOfExpression(i) - 1; // TODO this passed with and without -1; missing coverage?
                } else {
                    // This invocation cannot be inlined.
                    // TODO de-duplicate with the branch below
                    int expressionEnd = source.findEndOfExpression(i);
                    copyFrom(source, i);
                    inlineExpression(source, i + 1, expressionEnd, arguments, argumentsStart, null);
                    i = expressionEnd - 1; // Note: the for loop increments i // TODO consider just using a while loop
                }
            } else if (ExpressionType.isContainerStart(sourceElement.type)) {
                int expressionEnd = source.findEndOfExpression(i);
                copyFrom(source, i);
                inlineExpression(source, i + 1, expressionEnd, arguments, argumentsStart, null);
                i = expressionEnd - 1; // Note: the for loop increments i // TODO consider just using a while loop
            } else {
                // Scalar, container end, or None
                copyFrom(source, i);
            }
        }
        if (fieldName != null) {
            setFieldNameAt(startIndex, fieldName);
        }
    }

    private void add(Object context, byte type, Object value, String fieldName) {
        if (i >= core.elements.length) {
            core.grow();
        }
        Element element = core.elements[i];
        if (element == null) {
            element = new Element();
            core.elements[i] = element;
        }
        element.context = context;
        element.type = type;
        element.value = value;
        element.start = -1;
        element.end = -1;
        element.fieldName = fieldName;
        setExpressionStart(type);
        i++;
        if (i > core.size) { // Do not increase size if this was an in-place add.
            core.size++;
        }
        /*
        if (i >= core.size) {
            setExpressionStart(type);
            i++;
            core.size++;
        } else { // Do not increase size if this was an in-place add.
            i++;
        }

         */
    }

    private void copyElement(Core other, int otherIndex, boolean shouldCacheIndices) {
        Element element = core.elements[i];
        if (element == null) {
            element = new Element();
            core.elements[i] = element;
        }
        Element otherElement = other.elements[otherIndex];
        element.context = otherElement.context;
        element.type = otherElement.type;
        element.value = otherElement.value;
        element.start = otherElement.start;
        element.end = otherElement.end;
        element.fieldName = otherElement.fieldName;
        if (shouldCacheIndices) {
            setExpressionStart(element.type);
        }
    }

    void copyFromRange(Core other, int startIndex, int endIndex) {
        int copyLength = endIndex - startIndex;
        int destinationEnd = i + copyLength;
        if (destinationEnd >= insertionLimit) {
            //shiftRightToMakeRoom(destinationEnd - insertionLimit);
            lastAddOverflowed = true;
            return;
        }
        while (destinationEnd >= core.elements.length) {
            core.grow();
        }
        boolean shouldCacheIndices = destinationEnd > core.size;
        for (int otherIndex = startIndex; otherIndex < endIndex; otherIndex++) {
            copyElement(other, otherIndex, true);
            i++;
        }
        if (i > core.size) { // Do not increase size if this was an in-place copy.
            core.size += i - core.size;
        }
    }

    private void copyFrom(Core other, int otherIndex) {
        if (i >= core.elements.length) {
            core.grow();
        }
        copyElement(other, otherIndex, true);
        i++;
        if (i > core.size) { // Do not increase size if this was an in-place copy.
            core.size++;
        }
    }

    public void addScalar(IonType type, Object value, String fieldName) {
        add(NON_NULL_SCALAR_TYPE_IDS[type.ordinal()], ExpressionType.DATA_MODEL_SCALAR_ORDINAL, value, fieldName);
    }

    public void next() {
        i = iNext;
    }

    public void prepareNext() {
        iNext = i + 1;
    }

    public void skipTombstone() {
        iNext = core.elements[i].containerEnd; // When the type is TOMBSTONE, the end of the tombstone sequence is at containerEnd.
    }

    public int currentIndex() {
        return i;
    }

    public byte type() {
        return core.elements[i].type;
    }

    public IonType ionType() {
        return ((IonTypeID) core.elements[i].context).type;
    }

    public Object context() {
        return core.elements[i].context;
    }

    public int getEExpressionIndex() {
        return core.elements[i].end; // Note: it's up to the caller to ensure i points to an e-expression
    }

    public int size() {
        return core.size;
    }

    public void setFieldNameAt(int index, String fieldName) {
        core.elements[index].fieldName = fieldName;
    }

    public void rewindTo(int index) { // TODO make this just rewind(), always called with 0
        i = index;
        iNext = index;
        depth = 0;
    }

    public void prepareToOverwrite() {
        i = 0;
        iNext = 0;
        core.numberOfEExpressions = 0;
        depth = 0;
        Arrays.fill(core.numberOfExpressions, 0);
        currentDataModelContainerStart = 0;
        core.size = 0;
        //Arrays.fill(eExpressionActiveAtDepth, -1); // TODO check
        core.numberOfVariables = 0;
        //Arrays.fill(core.numberOfDuplicateUsagesByVariableOrdinal, 0);
        Arrays.fill(core.variableStartsByVariableOrdinal, null);
    }

    public void prepareToOverwriteAt(int index) {
        rewindTo(index);
        // TODO performance: cache depths by variable index
        currentDataModelContainerStart = core.elements[i].containerStart;
        int parentContainerIndex = currentDataModelContainerStart;
        // First, go back to the start of the depth 0 container.
        // TODO check that this doesn't always go back to 0
        while (parentContainerIndex > 0) {
            int grandparent = core.elements[parentContainerIndex].containerStart;
            if (grandparent >= 0) {
                parentContainerIndex = grandparent;
            }
        }
        // Now, calculate the depth of containers between the parent start and the destination.
        while (parentContainerIndex >= 0 && parentContainerIndex < i) {
            byte type = core.elements[parentContainerIndex].type;
            if (type == TOMBSTONE_ORDINAL) {
                parentContainerIndex = core.elements[parentContainerIndex].containerEnd;
            } else {
                if (ExpressionType.isContainerStart(type)) {
                    depth++;
                } else if (ExpressionType.isEnd(type)) {
                    depth--;
                }
                parentContainerIndex++;
            }
        }
        // TODO other attributes might be needed, or it might be depth (check eExpressionActiveAtDepth)
    }

    public boolean isExhausted() {
        return i >= core.size;
    }

    public void clear() {
        i = 0;
        iNext = 0;
        core.size = 0;
        Arrays.fill(core.numberOfExpressions, 0);
        Arrays.fill(eExpressionActiveAtDepth, -1);
        core.numberOfVariables = 0;
        depth = 0;
        core.numberOfEExpressions = 0;
        insertionLimit = Integer.MAX_VALUE;
    }

    public void setNextAfterEndOfEExpression(int eExpressionIndex) {
        iNext = core.eExpressionEnds[eExpressionIndex]; // TODO not accurate after tape reuse
    }

    public void advanceToAfterEndEExpression(int eExpressionIndex) {
        i = core.eExpressionEnds[eExpressionIndex];
        iNext = i;
    }

    public void advanceToAfterEndContainer() {
        if (isExhausted()) { // TODO is / why is this necessary?
            return;
        }
        if (core.elements[i].type == DATA_MODEL_CONTAINER_END_ORDINAL) {
            // TODO maybe this is an optimization, maybe not. Tried it for correctness; didn't work. Try with and without.
            i++;
            iNext = i;
            return;
        }
        i = core.elements[core.elements[i].containerStart].containerEnd + 1;
        iNext = i;
    }

    void shiftIndicesLeft(int shiftAmount) {
        // TODO note: another way to handle this is to just store the shift amount and perform the subtraction on
        //  the way out. Would need to test that the stored shift amount applied only when necessary and determine
        //  whether multiple shifts could be applied to the same index (I think not)
        for (int i = 0; i < core.size; i++) {
            Element element = core.elements[i];
            if (element.type != ExpressionType.E_EXPRESSION_ORDINAL && element.start >= 0) {
                element.start -= shiftAmount;
                element.end -= shiftAmount;
            }
        }
    }

    private void prepareToRead(IonType expectedType) {
        Element element = core.elements[i];
        IonTypeID typeId = (IonTypeID) element.context;
        if (expectedType != typeId.type) {
            throw new IonException(String.format("Expected type %s, but found %s.", expectedType, typeId.type));
        }
        reader.sliceAfterHeader(element.start, element.end, typeId);
    }

    public List<SymbolToken> annotations() {
        return (List<SymbolToken>) core.elements[i].context;
    }

    public String fieldName() {
        return core.elements[i].fieldName;
    }

    public boolean isNullValue() {
        IonTypeID typeId = (IonTypeID) core.elements[i].context;
        return typeId.isNull;
    }

    public boolean readBoolean() {
        Element element = core.elements[i];
        if (element.start < 0) {
            return (boolean) element.value;
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.BOOL);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        boolean value = reader.booleanValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public long readLong() {
        Element element = core.elements[i];
        if (element.start < 0) {
            if (element.value instanceof BigInteger) {
                return ((BigInteger) element.value).longValue();
            }
            return (long) element.value; // TODO add a nicer error if something is wrong
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.INT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        long value = reader.longValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public BigInteger readBigInteger() {
        Element element = core.elements[i];
        if (element.start < 0) {
            if (element.value instanceof BigInteger) {
                return (BigInteger) element.value;
            }
            return BigInteger.valueOf((long) element.value); // TODO add a nicer error if something is wrong
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.INT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        BigInteger value = reader.bigIntegerValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public IntegerSize readIntegerSize() {
        Element element = core.elements[i];
        if (element.start < 0) {
            if (element.value instanceof BigInteger) {
                return IntegerSize.BIG_INTEGER;
            }
            long longValue = (long) element.value;
            if (longValue <= Integer.MAX_VALUE && longValue >= Integer.MIN_VALUE) {
                return IntegerSize.INT;
            }
            return IntegerSize.LONG;
        }
        // TODO it's common to call getIntegerSize(), then the corresponding Value() method. The current implementation
        //  here involves a lot of duplicate work.
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.INT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        IntegerSize value = reader.getIntegerSize();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public BigDecimal readBigDecimal() {
        Element element = core.elements[i];
        if (element.start < 0) {
            return (BigDecimal) element.value;
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) element.context;
        if (typeId.type != IonType.INT && typeId.type != IonType.DECIMAL) {
            throw new IonException(String.format("Expected int or decimal, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(element.start, element.end, typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        Decimal value = reader.decimalValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public String readText() {
        Element element = core.elements[i];
        if (element.start < 0) {
            if (element.value instanceof SymbolToken) {
                return ((SymbolToken) element.value).assumeText();
            }
            return (String) element.value;
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) element.context;
        if (!IonType.isText(typeId.type)) {
            throw new IonException(String.format("Expected string or symbol, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(element.start, element.end, typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        String value = reader.stringValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public SymbolToken readSymbol() {
        Element element = core.elements[i];
        if (element.start < 0) {
            if (element.value instanceof SymbolToken) {
                return (SymbolToken) element.value;
            }
            return _Private_Utils.newSymbolToken((String) element.value);
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) element.context;
        if (!IonType.isText(typeId.type)) {
            throw new IonException(String.format("Expected string or symbol, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(element.start, element.end, typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        SymbolToken value;
        if (typeId.type == IonType.SYMBOL) {
            value = reader.symbolValue();
        } else {
            value = _Private_Utils.newSymbolToken(reader.stringValue());
        }
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public int lobSize() {
        Element element = core.elements[i];
        if (element.start < 0) {
            return ((byte[]) element.value).length;
        }
        IonTypeID typeId = (IonTypeID) element.context;
        if (!IonType.isLob(typeId.type)) {
            throw new IonException(String.format("Expected blob or clob, but found %s.", typeId.type));
        }
        return element.end - element.start;
    }

    public byte[] readLob() {
        Element element = core.elements[i];
        if (element.start < 0) {
            return (byte[]) element.value;
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        IonTypeID typeId = (IonTypeID) element.context;
        if (!IonType.isLob(typeId.type)) {
            throw new IonException(String.format("Expected blob or clob, but found %s.", typeId.type));
        }
        reader.sliceAfterHeader(element.start, element.end, typeId);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        byte[] value = reader.newBytes();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public double readFloat() {
        Element element = core.elements[i];
        if (element.start < 0) {
            return (double) element.value;
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.FLOAT);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        double value = reader.doubleValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    public Timestamp readTimestamp() {
        Element element = core.elements[i];
        if (element.start < 0) {
            return (Timestamp) element.value;
        }
        boolean isEvaluating = reader.isEvaluatingEExpression;
        reader.isEvaluatingEExpression = false; // TODO hack
        prepareToRead(IonType.TIMESTAMP);
        if (reader.isNullValue()) {
            throw new IonException("Expected a non-null value.");
        }
        Timestamp value = reader.timestampValue();
        reader.isEvaluatingEExpression = isEvaluating;
        return value;
    }

    // TODO might be able to get around needing these by adding an ExpressionType value for NULL_SCALAR
    private static final IonTypeID[] NON_NULL_SCALAR_TYPE_IDS;
    private static final IonTypeID[] NULL_SCALAR_TYPE_IDS;

    static {
        IonType[] ionTypes = IonType.values();
        NON_NULL_SCALAR_TYPE_IDS = new IonTypeID[ionTypes.length];
        NULL_SCALAR_TYPE_IDS = new IonTypeID[ionTypes.length];
        for (IonType type : ionTypes) {
            int ordinal = type.ordinal();
            if (type == IonType.DATAGRAM) {
                continue;
            }
            if (type == IonType.NULL) {
                NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.NULL_UNTYPED & 0xFF];
                continue;
            }
            NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.NULL_TYPE_IDS_1_1[ordinal - 1];
            switch (type) {
                case BOOL:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.BOOLEAN_TRUE & 0xFF];
                    break;
                case INT:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.INTEGER_ZERO_LENGTH & 0xFF];
                    break;
                case FLOAT:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.FLOAT_ZERO_LENGTH & 0xFF];
                    break;
                case DECIMAL:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.DECIMAL_ZERO_LENGTH & 0xFF];
                    break;
                case TIMESTAMP:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.TIMESTAMP_DAY_PRECISION & 0xFF];
                    break;
                case SYMBOL:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.SYMBOL_ADDRESS_1_BYTE & 0xFF];
                    break;
                case STRING:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.STRING_ZERO_LENGTH & 0xFF];
                    break;
                case CLOB:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.VARIABLE_LENGTH_CLOB & 0xFF];
                    break;
                case BLOB:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.VARIABLE_LENGTH_BLOB & 0xFF];
                    break;
                case LIST:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.LIST_ZERO_LENGTH & 0xFF];
                    break;
                case SEXP:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.SEXP_ZERO_LENGTH & 0xFF];
                    break;
                case STRUCT:
                    NON_NULL_SCALAR_TYPE_IDS[ordinal] = IonTypeID.TYPE_IDS_1_1[OpCodes.STRUCT_SID_ZERO_LENGTH & 0xFF];
                    break;
                default:
                    throw new IllegalStateException();
            }
        }

    }

    public void addDataModelValue(Expression.DataModelValue value, String fieldName) {
        List<SymbolToken> annotations = value.getAnnotations();
        if (!annotations.isEmpty()) {
            add(annotations, ExpressionType.ANNOTATION_ORDINAL, null, fieldName);
        }
        IonType type = value.getType();
        if (value instanceof Expression.NullValue) {
            add(NULL_SCALAR_TYPE_IDS[type.ordinal()], ExpressionType.DATA_MODEL_SCALAR_ORDINAL, null, fieldName);
        } else {
            IonTypeID typeID = NON_NULL_SCALAR_TYPE_IDS[type.ordinal()];
            switch (type) {
                case BOOL:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.BoolValue) value).getValue(), fieldName);
                    break;
                case INT:
                    if (value instanceof Expression.LongIntValue) {
                        add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.LongIntValue) value).getValue(), fieldName);
                    } else {
                        add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.BigIntValue) value).getValue(), fieldName);
                    }
                    break;
                case FLOAT:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.FloatValue) value).getValue(), fieldName);
                    break;
                case DECIMAL:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.DecimalValue) value).getValue(), fieldName);
                    break;
                case TIMESTAMP:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.TimestampValue) value).getValue(), fieldName);
                    break;
                case SYMBOL:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.SymbolValue) value).getValue(), fieldName);
                    break;
                case STRING:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.StringValue) value).getValue(), fieldName);
                    break;
                case CLOB:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.ClobValue) value).getValue(), fieldName);
                    break;
                case BLOB:
                    add(typeID, ExpressionType.DATA_MODEL_SCALAR_ORDINAL, ((Expression.BlobValue) value).getValue(), fieldName);
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
    }

    public static ExpressionTape.Core inlineNestedInvocations(ExpressionTape.Core tape) {
        ExpressionTape newTape = new ExpressionTape(null, tape.size + 64); // TODO arbitrary, adding some room in anticipation of inlining some invocations. Consider being more accurate
        newTape.inlineExpression(tape, 0, tape.size, null, -1, null);
        return newTape.core;
    }

    public static ExpressionTape.Core from(List<Expression> expressions) { // TODO possibly temporary, until Expression model is replaced
        // Note: this method doesn't really need to be efficient, as it happens (at most) once during compile time.
        // First pass: determine the required size of the tape. All container types require two tape expressions: one
        // start and one end. Everything else requires one tape expression.
        int tapeSize = 0;
        for (Expression expression : expressions) {
            if (expression instanceof Expression.InvokableExpression && ((Expression.InvokableExpression) expression).getMacro() == SystemMacro.None) {
                tapeSize += 1; // In the tape, None is represented as a special type, not as an invocation with a start/end.
            } else if (expression instanceof Expression.HasStartAndEnd) {
                tapeSize += 2;
            } else {
                tapeSize += 1;
            }
            if (expression instanceof Expression.DataModelValue) {
                Expression.DataModelValue value = (Expression.DataModelValue) expression;
                if (!value.getAnnotations().isEmpty()) {
                    tapeSize++;
                }
            }
        }

        ExpressionTape tape = new ExpressionTape(null, tapeSize);
        List<Byte>[] ends = new List[expressions.size() + 1];
        String fieldName = null;
        for (int i = 0; i < expressions.size(); i++) {
            Expression expression = expressions.get(i);
            List<Byte> endsAtExpressionIndex = ends[i];
            if (endsAtExpressionIndex != null) {
                for (int j = endsAtExpressionIndex.size() - 1; j >= 0; j--) {
                    tape.add(null, endsAtExpressionIndex.get(j), null, null);
                }
            }
            if (expression instanceof Expression.FieldName) {
                fieldName = ((Expression.FieldName) expression).getValue().getText();
                continue;
            } else if (expression instanceof Expression.InvokableExpression) {
                Expression.InvokableExpression eExpression = (Expression.InvokableExpression) expression;
                Macro macro = eExpression.getMacro();
                if (macro == SystemMacro.None) {
                    // Note: the fieldName is ignored, but it is not harmful and can aid debugging.
                    tape.add(null, NONE_ORDINAL, null, fieldName);
                } else {
                    tape.add(macro, ExpressionType.E_EXPRESSION_ORDINAL, null, fieldName);
                    List<Byte> endsAtEndIndex = ends[eExpression.getEndExclusive()];
                    if (endsAtEndIndex == null) {
                        endsAtEndIndex = new ArrayList<>();
                    }
                    endsAtEndIndex.add(ExpressionType.E_EXPRESSION_END_ORDINAL);
                    ends[eExpression.getEndExclusive()] = endsAtEndIndex;
                }
            } else if (expression instanceof Expression.ExpressionGroup) {
                Expression.ExpressionGroup group = (Expression.ExpressionGroup) expression;
                tape.add(null, ExpressionType.EXPRESSION_GROUP_ORDINAL, null, fieldName); // TODO could pass along the endIndex as context?
                List<Byte> endsAtEndIndex = ends[group.getEndExclusive()];
                if (endsAtEndIndex == null) {
                    endsAtEndIndex = new ArrayList<>();
                }
                endsAtEndIndex.add(ExpressionType.EXPRESSION_GROUP_END_ORDINAL);
                ends[group.getEndExclusive()] = endsAtEndIndex;
            } else if (expression instanceof Expression.DataModelContainer) {
                Expression.DataModelContainer container = ((Expression.DataModelContainer) expression);
                List<SymbolToken> annotations = container.getAnnotations();
                if (!annotations.isEmpty()) {
                    tape.add(annotations, ExpressionType.ANNOTATION_ORDINAL, null, fieldName);
                }
                tape.add(NON_NULL_SCALAR_TYPE_IDS[container.getType().ordinal()], ExpressionType.DATA_MODEL_CONTAINER_ORDINAL, null, fieldName); // TODO could pass along the endIndex as context?
                if (container.getEndExclusive() == i) {
                    // This is an empty container
                    tape.add(null, ExpressionType.DATA_MODEL_CONTAINER_END_ORDINAL, null, fieldName);
                } else {
                    List<Byte> endsAtEndIndex = ends[container.getEndExclusive()];
                    if (endsAtEndIndex == null) {
                        endsAtEndIndex = new ArrayList<>();
                    }
                    endsAtEndIndex.add(ExpressionType.DATA_MODEL_CONTAINER_END_ORDINAL);
                    ends[container.getEndExclusive()] = endsAtEndIndex;
                }
            } else if (expression instanceof Expression.DataModelValue) {
                Expression.DataModelValue value = ((Expression.DataModelValue) expression);
                tape.addDataModelValue(value, fieldName);
            } else if (expression instanceof Expression.VariableRef) {
                tape.add(((Expression.VariableRef) expression).getSignatureIndex(), ExpressionType.VARIABLE_ORDINAL, null, fieldName);
            } else {
                throw new IllegalStateException();
            }
            fieldName = null;
        }
        List<Byte> endsAtExpressionIndex = ends[expressions.size()];
        if (endsAtExpressionIndex != null) {
            for (int j = endsAtExpressionIndex.size() - 1; j >= 0; j--) {
                tape.add(null, endsAtExpressionIndex.get(j), null, null);
            }
        }
        tape.rewindTo(0);
        return tape.core;
    }

    public ExpressionTape seekToArgument(int eExpressionIndex, int indexRelativeToStart) {
        if (core.numberOfExpressions[eExpressionIndex] <= indexRelativeToStart) {
            return null;
        }
        i = core.expressionStarts[eExpressionIndex][indexRelativeToStart];
        iNext = i;
        return this;
    }

    /**
     * Transcodes the e-expression argument expressions provided to this MacroEvaluator
     * without evaluation.
     * @param writer the writer to which the expressions will be transcoded.
     */
    void transcodeArgumentsTo(MacroAwareIonWriter writer) throws IOException {
        int index = 0;
        rewindTo(0);
        while (index < size()) {
            next();
            prepareNext();
            byte argument = type();
            if (ExpressionType.isEnd(argument)) {
                writer.stepOut();
                index++;
                continue;
            }
            String fieldName = fieldName();
            if (fieldName != null) {
                writer.setFieldName(fieldName);
            }
            switch (argument) {
                case ExpressionType.ANNOTATION_ORDINAL:
                    writer.setTypeAnnotationSymbols(((List<SymbolToken>) context()).toArray(new SymbolToken[0]));
                    break;
                case ExpressionType.DATA_MODEL_CONTAINER_ORDINAL:
                    writer.stepIn(ionType());
                    break;
                case ExpressionType.DATA_MODEL_SCALAR_ORDINAL:
                    switch (ionType()) {
                        case NULL: writer.writeNull(); break;
                        case BOOL: writer.writeBool(readBoolean()); break;
                        case INT:
                            switch (readIntegerSize()) {
                                case INT: writer.writeInt(readLong()); break;
                                case LONG: writer.writeInt(readLong()); break;
                                case BIG_INTEGER: writer.writeInt(readBigInteger()); break;
                            }
                            break;
                        case FLOAT: writer.writeFloat(readFloat()); break;
                        case DECIMAL: writer.writeDecimal(readBigDecimal()); break;
                        case TIMESTAMP: writer.writeTimestamp(readTimestamp()); break;
                        case SYMBOL: writer.writeSymbolToken(readSymbol()); break;
                        case STRING: writer.writeString(readText()); break;
                        case BLOB: writer.writeBlob(readLob()); break;
                        case CLOB: writer.writeClob(readLob()); break;
                        default: throw new IllegalStateException("Unexpected branch");
                    }
                    break;
                case ExpressionType.E_EXPRESSION_ORDINAL:
                    writer.startMacro((Macro) context());
                    break;
                case ExpressionType.EXPRESSION_GROUP_ORDINAL:
                    writer.startExpressionGroup();
                    break;
                case NONE_ORDINAL:
                    writer.startMacro(SystemMacro.None);
                    writer.endMacro();
                    break;
                default: throw new IllegalStateException("Unexpected branch");
            }
            index++;
        }
        rewindTo(0);
    }
}
