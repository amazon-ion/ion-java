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

import com.amazon.ion.IonReader;
import com.amazon.ion.IonTestCase;
import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonTextWriterBuilder;
import java.io.IOException;
import java.io.StringWriter;
import org.junit.Test;

public class IonMarkupWriterTest extends IonTestCase {
    private String input     =
            "abcd::{ hello:( sexp 1 2 \"str\" ), 'list':[ 3.2, 32e-1], " +
            "blob:annot::" +
            "{{ T25lIEJpZyBGYXQgVGVzdCBCbG9iIEZvciBZb3VyIFBsZWFzdXJl }} }";
    private String sExpected =
            "<beforeAnnotations STRUCT><beforeEachAnnotation STRUCT>abcd" +
            "<afterEachAnnotation STRUCT>::<afterAnnotations STRUCT>" +
            "<beforeData STRUCT>{<afterStepIn STRUCT><beforeFieldName SEXP>" +
            "hello<afterFieldName SEXP>:<beforeData SEXP>(<afterStepIn SEXP>" +
            "<beforeData SYMBOL>sexp<afterData SYMBOL><beforeSeparator SEXP> " +
            "<afterSeparator SEXP><beforeData INT>1<afterData INT>" +
            "<beforeSeparator SEXP> <afterSeparator SEXP><beforeData INT>" +
            "2<afterData INT><beforeSeparator SEXP> <afterSeparator SEXP>" +
            "<beforeData STRING>\"str\"<afterData STRING><beforeStepOut SEXP>" +
            ")<afterData SEXP><beforeSeparator STRUCT>,<afterSeparator STRUCT>" +
            "<beforeFieldName LIST>list<afterFieldName LIST>:<beforeData LIST>" +
            "[<afterStepIn LIST><beforeData DECIMAL>3.2<afterData DECIMAL>" +
            "<beforeSeparator LIST>,<afterSeparator LIST><beforeData FLOAT>" +
            "3.2e0<afterData FLOAT><beforeStepOut LIST>]<afterData LIST>" +
            "<beforeSeparator STRUCT>,<afterSeparator STRUCT>" +
            "<beforeFieldName BLOB>blob<afterFieldName BLOB>:<beforeAnnotations" +
            " BLOB><beforeEachAnnotation BLOB>annot<afterEachAnnotation BLOB>::" +
            "<afterAnnotations BLOB><beforeData BLOB>" +
            "{{T25lIEJpZyBGYXQgVGVzdCBCbG9iIEZvciBZb3VyIFBsZWFzdXJl}}<afterData" +
            " BLOB><beforeStepOut STRUCT>}<afterData STRUCT>";
    // TODO amzn/ion-java/issues/57 determine if platform specific newlines are appropriate for pretty printing
    private String pExpected = String.format(
            "%n<beforeAnnotations STRUCT><beforeEachAnnotation STRUCT>abcd" +
            "<afterEachAnnotation STRUCT>::<afterAnnotations STRUCT><beforeData" +
            " STRUCT>{<afterStepIn STRUCT>%n  <beforeFieldName SEXP>hello" +
            "<afterFieldName SEXP>: <beforeData SEXP>(<afterStepIn SEXP>%n    " +
            "<beforeData SYMBOL>sexp<afterData SYMBOL><beforeSeparator SEXP>" +
            "%n    <afterSeparator SEXP><beforeData INT>1<afterData INT>" +
            "<beforeSeparator SEXP>%n    <afterSeparator SEXP><beforeData INT>2" +
            "<afterData INT><beforeSeparator SEXP>%n    <afterSeparator SEXP>" +
            "<beforeData STRING>\"str\"<afterData STRING><beforeStepOut SEXP>%n  " +
            ")<afterData SEXP><beforeSeparator STRUCT>,%n  " +
            "<afterSeparator STRUCT><beforeFieldName LIST>list<afterFieldName " +
            "LIST>: <beforeData LIST>[<afterStepIn LIST>%n    <beforeData " +
            "DECIMAL>3.2<afterData DECIMAL><beforeSeparator LIST>,%n    " +
            "<afterSeparator LIST><beforeData FLOAT>3.2e0<afterData FLOAT>" +
            "<beforeStepOut LIST>%n  ]<afterData LIST><beforeSeparator STRUCT>" +
            ",%n  <afterSeparator STRUCT><beforeFieldName BLOB>blob" +
            "<afterFieldName BLOB>: <beforeAnnotations BLOB>" +
            "<beforeEachAnnotation BLOB>annot<afterEachAnnotation BLOB>::" +
            "<afterAnnotations BLOB><beforeData BLOB>" +
            "{{ T25lIEJpZyBGYXQgVGVzdCBCbG9iIEZvciBZb3VyIFBsZWFzdXJl }}" +
            "<afterData BLOB><beforeStepOut STRUCT>%n}<afterData STRUCT>"
    );

    @Test
    public void testStandardCallback()
        throws IOException
    {
        // Write to a StringWriter for testing
        StringWriter out = new StringWriter();
        IonReader ionReader = system().newReader(input);

        _Private_IonTextWriterBuilder builder = (_Private_IonTextWriterBuilder)
            IonTextWriterBuilder.standard();
        IonWriter ionWriter = builder
            .withCallbackBuilder(new TestMarkupCallback.Builder())
            .build(out);

        write(ionReader, ionWriter);
        assertEquals("Markup callback with standard Ion Writer, " +
                     "error with data:\n" + input + "\n",
                     sExpected, out.toString());
    }

    @Test
    public void testPrettyCallback()
        throws IOException
    {
        // Write to a StringWriter for testing
        StringWriter out = new StringWriter();
        IonReader ionReader = system().newReader(input);
        _Private_IonTextWriterBuilder builder = (_Private_IonTextWriterBuilder)
            IonTextWriterBuilder.pretty();
        IonWriter ionWriter = builder
            .withCallbackBuilder(new TestMarkupCallback.Builder())
            .build(out);
        write(ionReader, ionWriter);
        assertEquals("Markup callback with pretty printing Ion Writer," +
                     " error with data:\n" + input + "\n",
                     pExpected, out.toString());
    }

    @Test
    public void testEscaping() throws IOException
    {
        String input = "These should be escaped <>&";
        String expected = "<><><>&&&<><><>\"These should be escaped &lt;&gt;&amp;\"";
        StringWriter out = new StringWriter();
        _Private_IonTextWriterBuilder builder = (_Private_IonTextWriterBuilder)
            IonTextWriterBuilder.standard();
        IonWriter ionWriter = builder
            .withCallbackBuilder(new EscapingCallback.Builder())
            .build(out);
        ionWriter.writeString(input);
        ionWriter.finish();
        assertEquals("Escaping failed, with data:\n" + input + "\n",
                     expected, out.toString());
    }

    public void write(IonReader ionReader, IonWriter ionWriter)
        throws IOException
    {
        assertNotNull("ionReader is null", ionReader);
        assertNotNull("ionWriter is null", ionWriter);

        ionWriter.writeValues(ionReader);

        ionWriter.close();
        ionReader.close();
    }
}
