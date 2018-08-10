package software.amazon.ion.impl;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.ion.Decimal;

import static org.junit.Assert.*;

public class PrivateScalarConversionsTest {

    private long decimalToLong(final Decimal d) {
        PrivateScalarConversions.ValueVariant v = new PrivateScalarConversions.ValueVariant();
        v.setValue(d);

        v.cast(PrivateScalarConversions.FNID_FROM_DECIMAL_TO_LONG);
        return v.getLong();
    }

    @Test
    public void decimalToLong() {
        assertEquals(1, decimalToLong(Decimal.valueOf(1L)));
    }

    @Test
    public void decimalToMinLong() {
        assertEquals(Long.MAX_VALUE, decimalToLong(Decimal.valueOf(Long.MAX_VALUE)));
    }

    @Test
    public void decimalToMaxLong() {
        assertEquals(Long.MIN_VALUE, decimalToLong(Decimal.valueOf(Long.MIN_VALUE)));
    }
}