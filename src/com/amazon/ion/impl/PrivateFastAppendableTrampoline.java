package com.amazon.ion.impl;

import com.amazon.ion.util.PrivateFastAppendable;
import java.io.OutputStream;


/**
 * @deprecated This is an internal API that is subject to change without notice.
 */
@Deprecated
public final class PrivateFastAppendableTrampoline
{

    public static PrivateFastAppendable forAppendable(Appendable appendable)
    {
        return new AppendableFastAppendable(appendable);
    }

    public static PrivateFastAppendable forOutputStream(
            OutputStream outputStream)
    {
        return new OutputStreamFastAppendable(outputStream);
    }
}
