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

import java.io.IOException;



/**
 * An Ion <code>blob</code> value.
 * <p>
 * <b>WARNING:</b> This interface should not be implemented or extended by
 * code outside of this library.
 */
public interface IonBlob
    extends IonLob
{
    /**
     * Prints the content of this blob as Base64 text, without Ion's
     * surrounding double-braces <code>{{ }}</code>.
     *
     * @param out will receive the Base64 content.
     * @throws NullValueException if <code>this.isNullValue()</code>.
     * @throws NullPointerException if <code>out</code> is null.
     * @throws IOException if there's a problem writing to the output stream.
     */
    public void printBase64(Appendable out)
        throws NullValueException, IOException;


    public IonBlob clone()
        throws UnknownSymbolException;
}
