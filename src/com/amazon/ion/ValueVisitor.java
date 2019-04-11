/*
 * Copyright 2007-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ion;

import com.amazon.ion.util.AbstractValueVisitor;

/**
 * A Visitor for the Ion value hierarchy.
 *
 * @see AbstractValueVisitor
 */
public interface ValueVisitor
{
    public void visit(IonBlob value) throws Exception;

    public void visit(IonBool value) throws Exception;

    public void visit(IonClob value) throws Exception;

    public void visit(IonDatagram value) throws Exception;

    public void visit(IonDecimal value) throws Exception;

    public void visit(IonFloat value) throws Exception;

    public void visit(IonInt value) throws Exception;

    public void visit(IonList value) throws Exception;

    public void visit(IonNull value) throws Exception;

    public void visit(IonSexp value) throws Exception;

    public void visit(IonString value) throws Exception;

    public void visit(IonStruct value) throws Exception;

    public void visit(IonSymbol value) throws Exception;

    public void visit(IonTimestamp value) throws Exception;
}