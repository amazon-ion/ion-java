
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

package com.amazon.ion.impl;

import com.amazon.ion.IonType;
import com.amazon.ion.SymbolToken;

import com.amazon.ion.system.IonTextWriterBuilder;
import com.amazon.ion.util._Private_FastAppendable;
import java.io.IOException;

/**
 * NOT FOR APPLICATION USE!
 *
 *
 * Callback for giving users the ability to inject markup into their Ion
 * documents.
 * <p>
 * Customers who want to inject markup into their Ion documents will need to
 * extend {@link _Private_MarkupCallback}, implement {@link _Private_CallbackBuilder} to build new
 * instances, and pass an instance of their {@link _Private_CallbackBuilder} into
 * {@link IonTextWriterBuilder#setCallbackBuilder(_Private_CallbackBuilder)} or
 * {@link IonTextWriterBuilder#withCallbackBuilder(_Private_CallbackBuilder)}
 * </p>
 * <p>
 * Note: It is only necessary for subclasses to implement methods they're using
 * to inject markup.
 * </p>
 * <p>
 * When all method are called, they are called with (at least) an
 * {@link IonType} that represents either the type of data being written or the
 * container type that the writer is writing, or currently inside.
 * before/afterFieldName and before/afterEachAnnotation get, respectively, the
 * field name and annotation that is being written. Methods can use
 * {@link #myAppendable} access the output stream.
 * </p>
 * <h3>Example Calls</h3>
 * <p>
 * Here are some example call patterns, using the following syntax to show where
 * calls will be executed (whitespace is used to increase readability):
 *
 * <pre>
 * &lt;methodName TYPE&gt;
 * </pre>
 *
 * </p>
 * <p>
 * Input:
 *
 * <pre>
 * { cookies:"Chocolate Chip" }
 * </pre>
 *
 * Output:
 *
 * <pre>
 * &lt;beforeData STRUCT&gt;{&lt;afterStepIn STRUCT&gt;
 *     &lt;beforeFieldName STRING "cookies"&gt; cookies &lt;afterFieldName STRING "cookies"&gt;:&lt;beforeData STRING&gt; "Chocolate Chip" &lt;afterData STRING&gt;
 * &lt;beforeStepOut STRUCT&gt;}&lt;afterData STRUCT&gt;
 * </pre>
 *
 * </p>
 * <p>
 * Input:
 *
 * <pre>
 * anno1::anno2::{
 *   fname:"John",
 *   lname:"Smith",
 *   age:32.785
 * }
 * </pre>
 *
 * Output:
 *
 * <pre>
 * &lt;beforeAnnotations STRUCT&gt;
 *   &lt;beforeEachAnnotation STRUCT "anno1"&gt;anno1&lt;afterEachAnnotation STRUCT "anno1"&gt;::
 *   &lt;beforeEachAnnotation STRUCT "anno2"&gt;anno2&lt;afterEachAnnotation STRUCT "anno2"&gt;::
 * &lt;afterAnnotations STRUCT&gt;
 * &lt;beforeData STRUCT&gt;{&lt;afterStepIn STRUCT&gt;
 *     &lt;beforeFieldName STRING "fname"&gt;fname&lt;afterFieldName STRING "fname"&gt;:&lt;beforeData STRING&gt;"John"&lt;afterData STRING&gt;&lt;beforeSeparator STRUCT&gt;,&lt;afterSeparator STRUCT&gt;
 *     &lt;beforeFieldName STRING "lname"&gt;lname&lt;afterFieldName STRING "lname"&gt;:&lt;beforeData STRING&gt;"Smith"&lt;afterData STRING&gt;&lt;beforeSeparator STRUCT&gt;,&lt;afterSeparator STRUCT&gt;
 *     &lt;beforeFieldName DECIMAL "age"&gt;age&lt;afterFieldName DECIMAL "age"&gt;:&lt;beforeData DECIMAL&gt;32.785&lt;afterData DECIMAL&gt;
 * &lt;beforeStepOut STRUCT&gt;}&lt;afterData STRUCT&gt;
 * </pre>
 *
 * </p>
 * <p>
 * Input:
 *
 * <pre>
 * (where (field (this) result) (== (field (curr) majorVersionString) "1.0"))
 * </pre>
 *
 * Output:
 *
 * <pre>
 * &lt;beforeData SEXP&gt;(&lt;afterStepIn SEXP&gt;
 *     &lt;beforeData SYMBOL&gt;where&lt;afterData SYMBOL&gt;&lt;beforeSeparator SEXP&gt; &lt;afterSeparator SEXP&gt;
 *     &lt;beforeData SEXP&gt;(&lt;afterStepIn SEXP&gt;
 *         &lt;beforeData SYMBOL&gt;field&lt;afterData SYMBOL&gt;&lt;beforeSeparator SEXP&gt; &lt;afterSeparator SEXP&gt;
 *         &lt;beforeData SEXP&gt;(&lt;afterStepIn SEXP&gt;
 *             &lt;beforeData SYMBOL&gt;this&lt;afterData SYMBOL&gt;
 *         &lt;beforeStepOut SEXP&gt;)&lt;afterData SEXP&gt;&lt;beforeSeparator SEXP&gt; &lt;afterSeparator SEXP&gt;
 *         &lt;beforeData SYMBOL&gt;result&lt;afterData SYMBOL&gt;
 *     &lt;beforeStepOut SEXP&gt;)&lt;afterData SEXP&gt;&lt;beforeSeparator SEXP&gt; &lt;afterSeparator SEXP&gt;
 *     &lt;beforeData SEXP&gt;(&lt;afterStepIn SEXP&gt;
 *         &lt;beforeData SYMBOL&gt;==&lt;afterData SYMBOL&gt;&lt;beforeSeparator SEXP&gt; &lt;afterSeparator SEXP&gt;
 *         &lt;beforeData SEXP&gt;(&lt;afterStepIn SEXP&gt;
 *             &lt;beforeData SYMBOL&gt;field&lt;afterData SYMBOL&gt;&lt;beforeSeparator SEXP&gt; &lt;afterSeparator SEXP&gt;
 *             &lt;beforeData SEXP&gt;(&lt;afterStepIn SEXP&gt;
 *                 &lt;beforeData SYMBOL&gt;curr&lt;afterData SYMBOL&gt;
 *             &lt;beforeStepOut SEXP&gt;)&lt;afterData SEXP&gt;&lt;beforeSeparator SEXP&gt; &lt;afterSeparator SEXP&gt;
 *             &lt;beforeData SYMBOL&gt;majorVersionString&lt;afterData SYMBOL&gt;
 *         &lt;beforeStepOut SEXP&gt;)&lt;afterData SEXP&gt;&lt;beforeSeparator SEXP&gt; &lt;afterSeparator SEXP&gt;
 *         &lt;beforeData STRING&gt;"1.0"&lt;afterData STRING&gt;
 *     &lt;beforeStepOut SEXP&gt;)&lt;afterData SEXP&gt;
 * &lt;beforeStepOut SEXP&gt;)&lt;afterData SEXP&gt;
 * </pre>
 *
 * </p>
 * <p>
 * Input:
 *
 * <pre>
 * 1 5 "Cheesecake" 3.2 true null 'baby tigers' 47e1
 * </pre>
 *
 * Output:
 *
 * <pre>
 * &lt;beforeData INT&gt;1&lt;afterData INT&gt;&lt;beforeSeparator DATAGRAM&gt; &lt;afterSeparator DATAGRAM&gt;
 * &lt;beforeData INT&gt;5&lt;afterData INT&gt;&lt;beforeSeparator DATAGRAM&gt; &lt;afterSeparator DATAGRAM&gt;
 * &lt;beforeData STRING&gt;"Cheesecake"&lt;afterData STRING&gt;&lt;beforeSeparator DATAGRAM&gt; &lt;afterSeparator DATAGRAM&gt;
 * &lt;beforeData DECIMAL&gt;3.2&lt;afterData DECIMAL&gt;&lt;beforeSeparator DATAGRAM&gt; &lt;afterSeparator DATAGRAM&gt;
 * &lt;beforeData BOOL&gt;true&lt;afterData BOOL&gt;&lt;beforeSeparator DATAGRAM&gt; &lt;afterSeparator DATAGRAM&gt;
 * &lt;beforeData NULL&gt;null&lt;afterData NULL&gt;&lt;beforeSeparator DATAGRAM&gt; &lt;afterSeparator DATAGRAM&gt;
 * &lt;beforeData SYMBOL&gt;'baby tigers'&lt;afterData SYMBOL&gt;&lt;beforeSeparator DATAGRAM&gt; &lt;afterSeparator DATAGRAM&gt;
 * &lt;beforeData FLOAT&gt;470.0e0&lt;afterData FLOAT&gt;
 * </pre>
 *
 * </p>
 * <p>
 * Input:
 *
 * <pre>
 * [true, 3.4, 3d6, 2.3e8, "string", '''multi-''' '''string''',Symbol, 'qSymbol',
 *     {{"clob data"}}, {{YmxvYiBkYXRh}}, 1970-06-06, null.struct]
 * </pre>
 *
 * Output:
 *
 * <pre>
 * &lt;beforeData LIST&gt;[&lt;afterStepIn LIST&gt;
 *     &lt;beforeData BOOL&gt;true&lt;afterData BOOL&gt;&lt;beforeSeparator LIST&gt;,&lt;afterSeparator LIST&gt;
 *     &lt;beforeData DECIMAL&gt;3.4&lt;afterData DECIMAL&gt;&lt;beforeSeparator LIST&gt;,&lt;afterSeparator LIST&gt;
 *     &lt;beforeData DECIMAL&gt;3d6&lt;afterData DECIMAL&gt;&lt;beforeSeparator LIST&gt;,&lt;afterSeparator LIST&gt;
 *     &lt;beforeData FLOAT&gt;2.3E8&lt;afterData FLOAT&gt;&lt;beforeSeparator LIST&gt;,&lt;afterSeparator LIST&gt;
 *     &lt;beforeData STRING&gt;"string"&lt;afterData STRING&gt;&lt;beforeSeparator LIST&gt;,&lt;afterSeparator LIST&gt;
 *     &lt;beforeData STRING&gt;"multi-string"&lt;afterData STRING&gt;&lt;beforeSeparator LIST&gt;,&lt;afterSeparator LIST&gt;
 *     &lt;beforeData SYMBOL&gt;Symbol&lt;afterData SYMBOL&gt;&lt;beforeSeparator LIST&gt;,&lt;afterSeparator LIST&gt;
 *     &lt;beforeData SYMBOL&gt;qSymbol&lt;afterData SYMBOL&gt;&lt;beforeSeparator LIST&gt;,&lt;afterSeparator LIST&gt;
 *     &lt;beforeData CLOB&gt;{{"clob data"}}&lt;afterData CLOB&gt;&lt;beforeSeparator LIST&gt;,&lt;afterSeparator LIST&gt;
 *     &lt;beforeData BLOB&gt;{{YmxvYiBkYXRh}}&lt;afterData BLOB&gt;&lt;beforeSeparator LIST&gt;,&lt;afterSeparator LIST&gt;
 *     &lt;beforeData TIMESTAMP&gt;1970-06-06&lt;afterData TIMESTAMP&gt;&lt;beforeSeparator LIST&gt;,&lt;afterSeparator LIST&gt;
 *     &lt;beforeData NULL&gt;null.struct&lt;afterData NULL&gt;
 * &lt;beforeStepOut LIST&gt;]&lt;afterData LIST&gt;
 * </pre>
 */
public abstract class _Private_MarkupCallback
{
    private final _Private_FastAppendable myAppendable;

    public _Private_MarkupCallback(_Private_FastAppendable appendable)
    {
        this.myAppendable = appendable;
    }

    /**
     * Gets the {@link _Private_FastAppendable} that IonWriter will use to write its
     * output.
     */
    public final _Private_FastAppendable getAppendable()
    {
        return myAppendable;
    }

    /**
     * Callback to be executed before an Ion value is written. If iType is a
     * container type, this is executed before the container's opening
     * delimiter. To write data after the opening delimiter use
     * {@link #afterStepIn(IonType)}.
     * @param iType
     *            The type of data that will be written.
     */
    public void beforeValue(IonType iType)
        throws IOException
    {
    }

    /**
     * Callback to be executed after an Ion value is written. If iType is a
     * container type, this is executed after the container's closing delimiter.
     * To write data before the closing delimiter, use
     * {@link #beforeStepOut(IonType)}.
     * @param iType
     *            The type of data that was written.
     */
    public void afterValue(IonType iType)
        throws IOException
    {
    }

    /**
     * Callback to be executed before a field name is written.
     * @param iType
     *            The type of data in the field name's corresponding value.
     * @param name
     *            The field name that is being written.
     */
    public void beforeFieldName(IonType iType, SymbolToken name)
        throws IOException
    {
    }

    /**
     * Callback to be executed after a field name is written.
     * @param iType
     *            The type of data in the field name's corresponding value.
     * @param name
     *            The field name that is being written.
     */
    public void afterFieldName(IonType iType, SymbolToken name)
        throws IOException
    {
    }

    /**
     * Callback to be executed after the opening delimiter of a container is
     * written. To write data before the opening delimiter, use
     * {@link #beforeValue(IonType)}.
     * @param containerType
     *            The type of container that was just stepped into.
     */
    public void afterStepIn(IonType containerType)
        throws IOException
    {
    }

    /**
     * Callback to be executed before the closing delimiter of a container is
     * written. To write data after the closing delimiter, use
     * {@link #afterValue(IonType)}.
     * @param containerType
     *            The type of container that is about to be stepped out of.
     */
    public void beforeStepOut(IonType containerType)
        throws IOException
    {
    }

    /**
     * Callback to be executed before a separator is written. Called after the
     * data has been written, and before the separator. It is called when inside
     * all container types, including Sexp, and when not inside a container. It
     * is not called after the last element in a container.
     * @param containerType
     *            The type of container the writer is currently in. When writing
     *            top-level values, the type is DATAGRAM.
     */
    public void beforeSeparator(IonType containerType)
        throws IOException
    {
    }

    /**
     * Callback to be executed after a separator has been written. Called just
     * after the separator is written. It is called when inside all container
     * types, including Sexp, and when not inside a container. It is not called
     * after the last element in a container.
     * @param containerType
     *            The type of container the writer is currently in. When writing
     *            top-level values, the type is DATAGRAM.
     */
    public void afterSeparator(IonType containerType)
        throws IOException
    {
    }

    /**
     * Callback to be executed before annotations are written. It is executed
     * once per set of annotations.
     * @param iType
     *            The type of the data whose annotations are being written.
     */
    public void beforeAnnotations(IonType iType)
        throws IOException
    {
    }

    /**
     * Callback to be executed after annotations are written. It is executed
     * once per set of annotations.
     * @param iType
     *            The type of the data whose annotations are being written.
     */
    public void afterAnnotations(IonType iType)
        throws IOException
    {
    }

    /**
     * Callback to be executed before each annotation is written. It is executed
     * once per annotation.
     * @param iType
     *            The type of the data whose annotation is being written.
     * @param annotation
     *            The annotation that is being written.
     */
    public void beforeEachAnnotation(IonType iType, SymbolToken annotation)
        throws IOException
    {
    }

    /**
     * Callback to be executed after each annotation is written. It is executed
     * once per annotation, before the delimiter.
     * @param iType
     *            The type of the data whose annotation is being written.
     * @param annotation
     *            The annotation that is being written.
     */
    public void afterEachAnnotation(IonType iType, SymbolToken annotation)
        throws IOException
    {
    }
}
