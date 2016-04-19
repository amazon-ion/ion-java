/*
 * Copyright 2007-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package software.amazon.ion;


/**
 * An error caused by processing an Ion input stream that ends in the middle of
 * a value.
 */
public class UnexpectedEofException
    extends IonException
{
    private static final long serialVersionUID = 1L;

    public UnexpectedEofException()
    {
        super();
    }

    /**
     * @param message
     * @param cause
     */
    public UnexpectedEofException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * @param message
     */
    public UnexpectedEofException(String message)
    {
        super(message);
    }

    /**
     * @param cause
     */
    public UnexpectedEofException(Throwable cause)
    {
        super(cause);
    }
}
