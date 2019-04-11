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

/**
 * An error caused by an operation that requires an exact match on an import
 * within the catalog.
 *
 * @see SymbolTable#isSubstitute()
 */
// TODO amzn/ion-java/issues/40 Provide some useful info to assist callers with handling this
//      exception. E.g. reference to the substitute import in violation.
public class SubstituteSymbolTableException
    extends IonException
{
    private static final long serialVersionUID = 2885122600422187914L;

    public SubstituteSymbolTableException()
    {
        super();
    }

    public SubstituteSymbolTableException(String message)
    {
        super(message);
    }
}
