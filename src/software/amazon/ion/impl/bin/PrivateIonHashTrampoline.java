package software.amazon.ion.impl.bin;

import software.amazon.ion.IonWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * NOT FOR APPLICATION USE!
 *
 * Exposes {@link IonRawBinaryWriter} functionality for use when creating Ion hashes.
 */
@Deprecated
public final class PrivateIonHashTrampoline
{
    public static IonWriter newIonWriter(ByteArrayOutputStream baos) throws IOException
    {
        return new IonRawBinaryWriter(
                new PooledBlockAllocatorProvider(),
                PrivateIonManagedBinaryWriterBuilder.DEFAULT_BLOCK_SIZE,
                baos,
                AbstractIonWriter.WriteValueOptimization.NONE,
                IonRawBinaryWriter.StreamCloseMode.CLOSE,
                IonRawBinaryWriter.StreamFlushMode.FLUSH,
                IonRawBinaryWriter.PreallocationMode.PREALLOCATE_0,
                false     // force floats to be encoded as binary64
        );
    }
}
