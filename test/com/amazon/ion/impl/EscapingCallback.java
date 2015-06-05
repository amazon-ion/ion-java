// Copyright (c) 2013-2015 Amazon.com, Inc. All rights reserved.

package com.amazon.ion.impl;

import com.amazon.ion.IonType;
import com.amazon.ion.util._Private_FastAppendable;
import java.io.IOException;

class EscapingCallback
        extends _Private_MarkupCallback
{
    private _Private_FastAppendable myAppendable;

    public EscapingCallback(_Private_FastAppendable unescaped)
    {
        super(new EscapingIonAppendable(unescaped));
        myAppendable = unescaped;
    }

    @Override
    public void beforeValue(IonType iType)
        throws IOException
    {
        myAppendable.append("<><><>&&&<><><>");
    }

    public static class Builder implements _Private_CallbackBuilder {
        public _Private_MarkupCallback build(_Private_FastAppendable rawOutput)
        {
            return new EscapingCallback(rawOutput);
        }
    }

    static class EscapingIonAppendable
            extends _Private_FastAppendableDecorator
    {
        public EscapingIonAppendable(_Private_FastAppendable output) {
            super(output);
        }

        @Override
        public Appendable append(char c)
            throws IOException
        {
            switch (c) {
                case '&': super.appendAscii("&amp;"); break;
                case '<': super.appendAscii("&lt;");  break;
                case '>': super.appendAscii("&gt;");  break;
                // could be non-ASCII
                default : super.append(c);            break;
            }
            return this;
        }

        @Override
        public Appendable append(CharSequence csq)
            throws IOException
        {
            append(csq, 0, csq.length());
            return this;
        }

        @Override
        public Appendable append(CharSequence csq, int start, int end)
            throws IOException
        {
            int curr_start = start;
            for (int i = start; i < end; ++i) {
                char c = csq.charAt(i);
                if(c == '&' || c == '<' || c == '>') {
                    super.append(csq, curr_start, i);
                    append(c);
                    curr_start = i + 1;
                }
            }
            super.append(csq, curr_start, end);
            return this;
        }

        @Override
        public void appendAscii(char c)
            throws IOException
        {
            switch (c) {
                case '&': super.appendAscii("&amp;"); break;
                case '<': super.appendAscii("&lt;");  break;
                case '>': super.appendAscii("&gt;");  break;
                default : super.appendAscii(c);       break;
            }
        }

        @Override
        public void appendAscii(CharSequence csq)
            throws IOException
        {
            appendAscii(csq, 0, csq.length());
        }

        @Override
        public void appendAscii(CharSequence csq, int start, int end)
            throws IOException
        {
            int curr_start = start;
            for (int i = start; i < end; ++i) {
                char c = csq.charAt(i);
                if(c == '&' || c == '<' || c == '>') {
                    super.appendAscii(csq, curr_start, i);
                    appendAscii(c);
                    curr_start = i + 1;
                }
            }
            super.appendAscii(csq, curr_start, end);
        }
    }
}
