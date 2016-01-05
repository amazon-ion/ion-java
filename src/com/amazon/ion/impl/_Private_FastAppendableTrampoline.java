package com.amazon.ion.impl;

import com.amazon.ion.util._Private_FastAppendable;
import java.io.OutputStream;


/**
 * <b>NOT FOR APPLICATION USE!</b>
 */
public final class _Private_FastAppendableTrampoline
{

    public static _Private_FastAppendable forAppendable(Appendable appendable)
    {
        return new AppendableFastAppendable(appendable);
    }

    public static _Private_FastAppendable forOutputStream(
            OutputStream outputStream)
    {
        return new OutputStreamFastAppendable(outputStream);
    }
}
