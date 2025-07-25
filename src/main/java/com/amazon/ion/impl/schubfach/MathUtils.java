/*
 * Copyright 2018-2020 Raffaello Giulietti
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package com.amazon.ion.impl.schubfach;

/**
 * This class exposes package private utilities for other classes.
 * Thus, all methods are assumed to be invoked with correct arguments,
 * so these are not checked at all.
 *
 * @author Raffaello Giulietti
 */
final class MathUtils {
    /*
    For full details about this code see the following reference:

        Giulietti, "The Schubfach way to render doubles",
        https://drive.google.com/open?id=1luHhyQF9zKlM8yJ1nebU0OgVYhfC6CBN
     */
     
    /**
     * Returns the high 64 bits of the 128-bit product of two 64-bit factors.
     * This is an implementation of the Math.multiplyHigh method introduced in Java 9,
     * provided for compatibility with Java 8.
     *
     * @param x the first value
     * @param y the second value
     * @return the high 64 bits of the 128-bit product of the two arguments
     */
    static long multiplyHigh(long x, long y) {
        // Use the "classic" long multiplication algorithm
        long x1 = x >>> 32;
        long x2 = x & 0xFFFFFFFFL;
        long y1 = y >>> 32;
        long y2 = y & 0xFFFFFFFFL;

        long z2 = x2 * y2;
        long t = x1 * y2 + (z2 >>> 32);
        long z1 = t & 0xFFFFFFFFL;
        long z0 = t >>> 32;
        z1 += x2 * y1;
        return x1 * y1 + z0 + (z1 >>> 32);
    }

    /*
    The boundaries for k in g0(int) and g1(int).
    K_MIN must be DoubleToDecimal.K_MIN or less.
    K_MAX must be DoubleToDecimal.K_MAX or more.
     */
    static final int K_MIN = -324;
    static final int K_MAX = 292;

    // Must be DoubleToDecimal.H or more
    static final int H = 17;

    // C_10 = floor(log10(2) * 2^Q_10), A_10 = floor(log10(3/4) * 2^Q_10)
    private static final int Q_10 = 41;
    private static final long C_10 = 661_971_961_083L;
    private static final long A_10 = -274_743_187_321L;

    // C_2 = floor(log2(10) * 2^Q_2)
    private static final int Q_2 = 38;
    private static final long C_2 = 913_124_641_741L;

    private MathUtils() {
    }

    // The first powers of 10. The last entry must be 10^H.
    private static final long[] pow10 = {
        1L,
        10L,
        100L,
        1_000L,
        10_000L,
        100_000L,
        1_000_000L,
        10_000_000L,
        100_000_000L,
        1_000_000_000L,
        10_000_000_000L,
        100_000_000_000L,
        1_000_000_000_000L,
        10_000_000_000_000L,
        100_000_000_000_000L,
        1_000_000_000_000_000L,
        10_000_000_000_000_000L,
        100_000_000_000_000_000L,
    };

    /**
     * Returns 10<sup>{@code e}</sup>.
     *
     * @param e The exponent which must meet
     *          0 &le; {@code e} &le; {@link #H}.
     * @return 10<sup>{@code e}</sup>.
     */
    static long pow10(int e) {
        return pow10[e];
    }

    /**
     * Returns the unique integer <i>k</i> such that
     * 10<sup><i>k</i></sup> &le; 2<sup>{@code e}</sup>
     * &lt; 10<sup><i>k</i>+1</sup>.
     * <p>
     * The result is correct when |{@code e}| &le; 5_456_721.
     * Otherwise the result is undefined.
     *
     * @param e The exponent of 2, which should meet
     *          |{@code e}| &le; 5_456_721 for safe results.
     * @return &lfloor;log<sub>10</sub>2<sup>{@code e}</sup>&rfloor;.
     */
    static int flog10pow2(int e) {
        return (int) (e * C_10 >> Q_10);
    }

    /**
     * Returns the unique integer <i>k</i> such that
     * 10<sup><i>k</i></sup> &le; 3/4 &middot; 2<sup>{@code e}</sup>
     * &lt; 10<sup><i>k</i>+1</sup>.
     * <p>
     * The result is correct when
     * -2_956_395 &le; {@code e} &le; 2_500_325.
     * Otherwise the result is undefined.
     *
     * @param e The exponent of 2, which should meet
     *          -2_956_395 &le; {@code e} &le; 2_500_325 for safe results.
     * @return &lfloor;log<sub>10</sub>(3/4 &middot;
     * 2<sup>{@code e}</sup>)&rfloor;.
     */
    static int flog10threeQuartersPow2(int e) {
        return (int) (e * C_10 + A_10 >> Q_10);
    }

    /**
     * Returns the unique integer <i>k</i> such that
     * 2<sup><i>k</i></sup> &le; 10<sup>{@code e}</sup>
     * &lt; 2<sup><i>k</i>+1</sup>.
     * <p>
     * The result is correct when |{@code e}| &le; 1_838_394.
     * Otherwise the result is undefined.
     *
     * @param e The exponent of 10, which should meet
     *          |{@code e}| &le; 1_838_394 for safe results.
     * @return &lfloor;log<sub>2</sub>10<sup>{@code e}</sup>&rfloor;.
     */
    static int flog2pow10(int e) {
        return (int) (e * C_2 >> Q_2);
    }

    /**
     * Let 10<sup>-{@code k}</sup> = <i>&beta;</i> 2<sup><i>r</i></sup>,
     * for the unique pair of integer <i>r</i> and real <i>&beta;</i> meeting
     * 2<sup>125</sup> &le; <i>&beta;</i> &lt; 2<sup>126</sup>.
     * Further, let <i>g</i> = &lfloor;<i>&beta;</i>&rfloor; + 1.
     * Split <i>g</i> into the higher 63 bits <i>g</i><sub>1</sub> and
     * the lower 63 bits <i>g</i><sub>0</sub>. Thus,
     * <i>g</i><sub>1</sub> =
     * &lfloor;<i>g</i> 2<sup>-63</sup>&rfloor;
     * and
     * <i>g</i><sub>0</sub> =
     * <i>g</i> - <i>g</i><sub>1</sub> 2<sup>63</sup>.
     * <p>
     * This method returns <i>g</i><sub>1</sub> while
     * {@link #g0(int)} returns <i>g</i><sub>0</sub>.
     * <p>
     * If needed, the exponent <i>r</i> can be computed as
     * <i>r</i> = {@code flog2pow10(-k)} - 125 (see {@link #flog2pow10(int)}).
     *
     * @param k The exponent of 10, which must meet
     *          {@link #K_MIN} &le; {@code e} &le; {@link #K_MAX}.
     * @return <i>g</i><sub>1</sub> as described above.
     */
    static long g1(int k) {
        return g[k - K_MIN << 1];
    }

    /**
     * Returns <i>g</i><sub>0</sub> as described in
     * {@link #g1(int)}.
     *
     * @param k The exponent of 10, which must meet
     *          {@link #K_MIN} &le; {@code e} &le; {@link #K_MAX}.
     * @return <i>g</i><sub>0</sub> as described in
     * {@link #g1(int)}.
     */
    static long g0(int k) {
        return g[k - K_MIN << 1 | 1];
    }

    /*
    The precomputed values for g1(int) and g0(int).
    The first entry must be for an exponent of K_MIN or less.
    The last entry must be for an exponent of K_MAX or more.
     */
    private static final long[] g = {
        /* -324 */ 0x4F0C_EDC9_5A71_8DD4L, 0x5B01_E8B0_9AA0_D1B5L,
        /* -323 */ 0x7E7B_160E_F71C_1621L, 0x119C_A780_F767_B5EEL,
        /* -322 */ 0x652F_44D8_C5B0_11B4L, 0x0E16_EC67_2C52_F7F2L,
        /* -321 */ 0x50F2_9D7A_37C0_0E29L, 0x5812_56B8_F042_5FF5L,
        /* -320 */ 0x40C2_1794_F966_71BAL, 0x79A8_4560_C035_1991L,
        /* -319 */ 0x679C_F287_F570_B5F7L, 0x75DA_089A_CD21_C281L,
        /* -318 */ 0x52E3_F539_9126_F7F9L, 0x44AE_6D48_A41B_0201L,
        /* -317 */ 0x424F_F761_40EB_F994L, 0x36F1_F106_E9AF_34CDL,
        /* -316 */ 0x6A19_8BCE_CE46_5C20L, 0x57E9_81A4_A918_547BL,
        /* -315 */ 0x54E1_3CA5_71D1_E34DL, 0x2CBA_CE1D_5413_76C9L,
        /* -314 */ 0x43E7_63B7_8E41_82A4L, 0x23C8_A4E4_4342_C56EL,
        /* -313 */ 0x6CA5_6C58_E39C_043AL, 0x060D_D4A0_6B9E_08B0L,
        /* -312 */ 0x56EA_BD13_E949_9CFBL, 0x1E71_76E6_BC7E_6D59L,
        /* -311 */ 0x4588_9743_2107_B0C8L, 0x7EC1_2BEB_C9FE_BDE1L,
        /* -310 */ 0x6F40_F205_01A5_E7A7L, 0x7E01_DFDF_A997_9635L,
        /* -309 */ 0x5900_C19D_9AEB_1FB9L, 0x4B34_B319_5479_44F7L,
        /* -308 */ 0x4733_CE17_AF22_7FC7L, 0x55C3_C27A_A9FA_9D93L,
        /* -307 */ 0x71EC_7CF2_B1D0_CC72L, 0x5606_03F7_765D_C8EAL,
        /* -306 */ 0x5B23_9728_8E40_A38EL, 0x7804_CFF9_2B7E_3A55L,
        /* -305 */ 0x48E9_45BA_0B66_E93FL, 0x1337_0CC7_55FE_9511L,
        /* -304 */ 0x74A8_6F90_123E_41FEL, 0x51F1_AE0B_BCCA_881BL,
        /* -303 */ 0x5D53_8C73_41CB_67FEL, 0x74C1_5809_63D5_39AFL,
        /* -302 */ 0x4AA9_3D29_016F_8665L, 0x43CD_E007_8310_FAF3L,
        /* -301 */ 0x7775_2EA8_024C_0A3CL, 0x0616_333F_381B_2B1EL,
        /* -300 */ 0x5F90_F220_01D6_6E96L, 0x3811_C298_F9AF_55B1L,
        /* -299 */ 0x4C73_F4E6_67DE_BEDEL, 0x600E_3547_2E25_DE28L,
        /* -298 */ 0x7A53_2170_A631_3164L, 0x3349_EED8_49D6_303FL,
        /* -297 */ 0x61DC_1AC0_84F4_2783L, 0x42A1_8BE0_3B11_C033L,
        /* -296 */ 0x4E49_AF00_6A5C_EC69L, 0x1BB4_6FE6_95A7_CCF5L,
        /* -295 */ 0x7D42_B19A_43C7_E0A8L, 0x2C53_E63D_BC3F_AE55L,
        /* -294 */ 0x6435_5AE1_CFD3_1A20L, 0x2376_51CA_FCFF_BEAAL,
        /* -293 */ 0x502A_AF1B_0CA8_E1B3L, 0x35F8_416F_30CC_9888L,
        /* -292 */ 0x4022_25AF_3D53_E7C2L, 0x5E60_3458_F3D6_E06DL,
        /* -291 */ 0x669D_0918_621F_D937L, 0x4A33_86F4_B957_CD7BL,
        /* -290 */ 0x5217_3A79_E819_7A92L, 0x6E8F_9F2A_2DDF_D796L,
        /* -289 */ 0x41AC_2EC7_ECE1_2EDBL, 0x720C_7F54_F17F_DFABL,
        /* -288 */ 0x6913_7E0C_AE35_17C6L, 0x1CE0_CBBB_1BFF_CC45L,
        /* -287 */ 0x540F_980A_24F7_4638L, 0x171A_3C95_AFFF_D69EL,
        /* -286 */ 0x433F_ACD4_EA5F_6B60L, 0x127B_63AA_F333_1218L,
        /* -285 */ 0x6B99_1487_DD65_7899L, 0x6A5F_05DE_51EB_5026L,
        /* -284 */ 0x5614_106C_B11D_FA14L, 0x5518_D17E_A7EF_7352L,
        /* -283 */ 0x44DC_D9F0_8DB1_94DDL, 0x2A7A_4132_1FF2_C2A8L,
        /* -282 */ 0x6E2E_2980_E2B5_BAFBL, 0x5D90_6850_331E_043FL,
        /* -281 */ 0x5824_EE00_B55E_2F2FL, 0x6473_86A6_8F4B_3699L,
        /* -280 */ 0x4683_F19A_2AB1_BF59L, 0x36C2_D21E_D908_F87BL,
        /* -279 */ 0x70D3_1C29_DDE9_3228L, 0x579E_1CFE_280E_5A5DL,
        /* -278 */ 0x5A42_7CEE_4B20_F4EDL, 0x2C7E_7D98_200B_7B7EL,
        /* -277 */ 0x4835_30BE_A280_C3F1L, 0x09FE_CAE0_19A2_C932L,
        /* -276 */ 0x7388_4DFD_D0CE_064EL, 0x4331_4499_C29E_0EB6L,
        /* -275 */ 0x5C6D_0B31_73D8_050BL, 0x4F5A_9D47_CEE4_D891L,
        /* -274 */ 0x49F0_D5C1_2979_9DA2L, 0x72AE_E439_7250_AD41L,
        /* -273 */ 0x764E_22CE_A8C2_95D1L, 0x377E_39F5_83B4_4868L,
        /* -272 */ 0x5EA4_E8A5_53CE_DE41L, 0x12CB_6191_3629_D387L,
        /* -271 */ 0x4BB7_2084_430B_E500L, 0x756F_8140_F821_7605L,
        /* -270 */ 0x7925_00D3_9E79_6E67L, 0x6F18_CECE_59CF_233CL,
        /* -269 */ 0x60EA_670F_B1FA_BEB9L, 0x3F47_0BD8_47D8_E8FDL,
        /* -268 */ 0x4D88_5272_F4C8_9894L, 0x329F_3CAD_0647_20CAL,
        /* -267 */ 0x7C0D_50B7_EE0D_C0EDL, 0x3765_2DE1_A3A5_0143L,
        /* -266 */ 0x633D_DA2C_BE71_6724L, 0x2C50_F181_4FB7_3436L,
        /* -265 */ 0x4F64_AE8A_31F4_5283L, 0x3D0D_8E01_0C92_902BL,
        /* -264 */ 0x7F07_7DA9_E986_EA6BL, 0x7B48_E334_E0EA_8045L,
        /* -263 */ 0x659F_97BB_2138_BB89L, 0x4907_1C2A_4D88_669DL,
        /* -262 */ 0x514C_7962_80FA_2FA1L, 0x20D2_7CEE_A46D_1EE4L,
        /* -261 */ 0x4109_FAB5_33FB_594DL, 0x670E_CA58_838A_7F1DL,
        /* -260 */ 0x680F_F788_532B_C216L, 0x0B4A_DD5A_6C10_CB62L,
        /* -259 */ 0x533F_F939_DC23_01ABL, 0x22A2_4AAE_BCDA_3C4EL,
        /* -258 */ 0x4299_942E_49B5_9AEFL, 0x354E_A225_63E1_C9D8L,
        /* -257 */ 0x6A8F_537D_42BC_2B18L, 0x554A_9D08_9FCF_A95AL,
        /* -256 */ 0x553F_75FD_CEFC_EF46L, 0x776E_E406_E63F_BAAEL,
        /* -255 */ 0x4432_C4CB_0BFD_8C38L, 0x5F8B_E99F_1E99_6225L,
        /* -254 */ 0x6D1E_07AB_4662_79F4L, 0x3279_75CB_6428_9D08L,
        /* -253 */ 0x574B_3955_D1E8_6190L, 0x2861_2B09_1CED_4A6DL,
        /* -252 */ 0x45D5_C777_DB20_4E0DL, 0x06B4_226D_B0BD_D524L,
        /* -251 */ 0x6FBC_7259_5E9A_167BL, 0x2453_6A49_1AC9_5506L,
        /* -250 */ 0x5963_8EAD_E548_11FCL, 0x1D0F_883A_7BD4_4405L,
        /* -249 */ 0x4782_D88B_1DD3_4196L, 0x4A72_D361_FCA9_D004L,
        /* -248 */ 0x726A_F411_C952_028AL, 0x43EA_EBCF_FAA9_4CD3L,
        /* -247 */ 0x5B88_C341_6DDB_353BL, 0x4FEF_230C_C887_70A9L,
        /* -246 */ 0x493A_35CD_F17C_2A96L, 0x0CBF_4F3D_6D39_26EEL,
        /* -245 */ 0x7529_EFAF_E8C6_AA89L, 0x6132_1862_485B_717CL,
        /* -244 */ 0x5DBB_2626_53D2_2207L, 0x675B_46B5_06AF_8DFDL,
        /* -243 */ 0x4AFC_1E85_0FDB_4E6CL, 0x52AF_6BC4_0559_3E64L,
        /* -242 */ 0x77F9_CA6E_7FC5_4A47L, 0x377F_12D3_3BC1_FD6DL,
        /* -241 */ 0x5FFB_0858_6637_6E9FL, 0x45FF_4242_9634_CABDL,
        /* -240 */ 0x4CC8_D379_EB5F_8BB2L, 0x6B32_9B68_782A_3BCBL,
        /* -239 */ 0x7ADA_EBF6_4565_AC51L, 0x2B84_2BDA_59DD_2C77L,
        /* -238 */ 0x6248_BCC5_0451_56A7L, 0x3C69_BCAE_AE4A_89F9L,
        /* -237 */ 0x4EA0_9704_0374_4552L, 0x6387_CA25_583B_A194L,
        /* -236 */ 0x7DCD_BE6C_D253_A21EL, 0x05A6_103B_C05F_68EDL,
        /* -235 */ 0x64A4_9857_0EA9_4E7EL, 0x37B8_0CFC_99E5_ED8AL,
        /* -234 */ 0x5083_AD12_7221_0B98L, 0x2C93_3D96_E184_BE08L,
        /* -233 */ 0x4069_5741_F4E7_3C79L, 0x7075_CADF_1AD0_9807L,
        /* -232 */ 0x670E_F203_2171_FA5CL, 0x4D89_4498_2AE7_59A4L,
        /* -231 */ 0x5272_5B35_B45B_2EB0L, 0x3E07_6A13_5585_E150L,
        /* -230 */ 0x41F5_15C4_9048_F226L, 0x64D2_BB42_AAD1_810DL,
        /* -229 */ 0x6988_22D4_1A0E_503EL, 0x07B7_9204_4482_6815L,
        /* -228 */ 0x546C_E8A9_AE71_D9CBL, 0x1FC6_0E69_D068_5344L,
        /* -227 */ 0x438A_53BA_F1F4_AE3CL, 0x196B_3EBB_0D20_429DL,
        /* -226 */ 0x6C10_85F7_E987_7D2DL, 0x0F11_FDF8_1500_6A94L,
        /* -225 */ 0x5673_9E5F_EE05_FDBDL, 0x58DB_3193_4400_5543L,
        /* -224 */ 0x4529_4B7F_F19E_6497L, 0x60AF_5ADC_3666_AA9CL,
        /* -223 */ 0x6EA8_78CC_B5CA_3A8CL, 0x344B_C493_8A3D_DDC7L,
        /* -222 */ 0x5886_C70A_2B08_2ED6L, 0x5D09_6A0F_A1CB_17D2L,
        /* -221 */ 0x46D2_38D4_EF39_BF12L, 0x173A_BB3F_B4A2_7975L,
        /* -220 */ 0x7150_5AEE_4B8F_981DL, 0x0B91_2B99_2103_F588L,
        /* -219 */ 0x5AA6_AF25_093F_ACE4L, 0x0940_EFAD_B403_2AD3L,
        /* -218 */ 0x4885_58EA_6DCC_8A50L, 0x0767_2624_9002_88A9L,
        /* -217 */ 0x7408_8E43_E2E0_DD4CL, 0x723E_A36D_B337_410EL,
        /* -216 */ 0x5CD3_A503_1BE7_1770L, 0x5B65_4F8A_F5C5_CDA5L,
        /* -215 */ 0x4A42_EA68_E31F_45F3L, 0x62B7_72D5_916B_0AEBL,
        /* -214 */ 0x76D1_770E_3832_0986L, 0x0458_B7BC_1BDE_77DDL,
        /* -213 */ 0x5F0D_F8D8_2CF4_D46BL, 0x1D13_C630_164B_9318L,
        /* -212 */ 0x4C0B_2D79_BD90_A9EFL, 0x30DC_9E8C_DEA2_DC13L,
        /* -211 */ 0x79AB_7BF5_FC1A_A97FL, 0x0160_FDAE_3104_9351L,
        /* -210 */ 0x6155_FCC4_C9AE_EDFFL, 0x1AB3_FE24_F403_A90EL,
        /* -209 */ 0x4DDE_63D0_A158_BE65L, 0x6229_981D_9002_EDA5L,
        /* -208 */ 0x7C97_061A_9BC1_30A2L, 0x69DC_2695_B337_E2A1L,
        /* -207 */ 0x63AC_04E2_1634_26E8L, 0x54B0_1EDE_28F9_821BL,
        /* -206 */ 0x4FBC_D0B4_DE90_1F20L, 0x43C0_18B1_BA61_34E2L,
        /* -205 */ 0x7F94_8121_6419_CB67L, 0x1F99_C11C_5D68_549DL,
        /* -204 */ 0x6610_674D_E9AE_3C52L, 0x4C7B_00E3_7DED_107EL,
        /* -203 */ 0x51A6_B90B_2158_3042L, 0x09FC_00B5_FE57_4065L,
        /* -202 */ 0x4152_2DA2_8113_59CEL, 0x3B30_0091_9845_CD1DL,
        /* -201 */ 0x6883_7C37_34EB_C2E3L, 0x784C_CDB5_C06F_AE95L,
        /* -200 */ 0x539C_635F_5D89_68B6L, 0x2D0A_3E2B_0059_5877L,
        /* -199 */ 0x42E3_82B2_B13A_BA2BL, 0x3DA1_CB55_99E1_1393L,
        /* -198 */ 0x6B05_9DEA_B52A_C378L, 0x629C_7888_F634_EC1EL,
        /* -197 */ 0x559E_17EE_F755_692DL, 0x3549_FA07_2B5D_89B1L,
        /* -196 */ 0x447E_798B_F911_20F1L, 0x1107_FB38_EF7E_07C1L,
        /* -195 */ 0x6D97_28DF_F4E8_34B5L, 0x01A6_5EC1_7F30_0C68L,
        /* -194 */ 0x57AC_20B3_2A53_5D5DL, 0x4E1E_B234_65C0_09EDL,
        /* -193 */ 0x4623_4D5C_21DC_4AB1L, 0x24E5_5B5D_1E33_3B24L,
        /* -192 */ 0x7038_7BC6_9C93_AAB5L, 0x216E_F894_FD1E_C506L,
        /* -191 */ 0x59C6_C96B_B076_222AL, 0x4DF2_6077_30E5_6A6CL,
        /* -190 */ 0x47D2_3ABC_8D2B_4E88L, 0x3E5B_805F_5A51_21F0L,
        /* -189 */ 0x72E9_F794_1512_1740L, 0x63C5_9A32_2A1B_697FL,
        /* -188 */ 0x5BEE_5FA9_AA74_DF67L, 0x0304_7B5B_54E2_BACCL,
        /* -187 */ 0x498B_7FBA_EEC3_E5ECL, 0x0269_FC49_10B5_623DL,
        /* -186 */ 0x75AB_FF91_7E06_3CACL, 0x6A43_2D41_B455_69FBL,
        /* -185 */ 0x5E23_32DA_CB38_308AL, 0x21CF_5767_C377_87FCL,
        /* -184 */ 0x4B4F_5BE2_3C2C_F3A1L, 0x67D9_12B9_692C_6CCAL,
        /* -183 */ 0x787E_F969_F9E1_85CFL, 0x595B_5128_A847_1476L,
        /* -182 */ 0x6065_9454_C7E7_9E3FL, 0x6115_DA86_ED05_A9F8L,
        /* -181 */ 0x4D1E_1043_D31F_B1CCL, 0x4DAB_1538_BD9E_2193L,
        /* -180 */ 0x7B63_4D39_51CC_4FADL, 0x62AB_5527_95C9_CF52L,
        /* -179 */ 0x62B5_D761_0E3D_0C8BL, 0x0222_AA86_116E_3F75L,
        /* -178 */ 0x4EF7_DF80_D830_D6D5L, 0x4E82_2204_DABE_992AL,
        /* -177 */ 0x7E59_659A_F381_57BCL, 0x1736_9CD4_9130_F510L,
        /* -176 */ 0x6514_5148_C2CD_DFC9L, 0x5F5E_E3DD_40F3_F740L,
        /* -175 */ 0x50DD_0DD3_CF0B_196EL, 0x1918_B64A_9A5C_C5CDL,
        /* -174 */ 0x40B0_D7DC_A5A2_7ABEL, 0x4746_F83B_AEB0_9E3EL,
        /* -173 */ 0x6781_5961_0903_F797L, 0x253E_59F9_1780_FD2FL,
        /* -172 */ 0x52CD_E11A_6D9C_C612L, 0x50FE_AE60_DF9A_6426L,
        /* -171 */ 0x423E_4DAE_BE17_04DBL, 0x5A65_584D_7FAE_B685L,
        /* -170 */ 0x69FD_4917_968B_3AF9L, 0x10A2_26E2_65E4_573BL,
        /* -169 */ 0x54CA_A0DF_ABA2_9594L, 0x0D4E_8581_EB1D_1295L,
        /* -168 */ 0x43D5_4D7F_BC82_1143L, 0x243E_D134_BC17_4211L,
        /* -167 */ 0x6C88_7BFF_9403_4ED2L, 0x06CA_E854_6025_3682L,
        /* -166 */ 0x56D3_9666_1002_A574L, 0x6BD5_86A9_E684_2B9BL,
        /* -165 */ 0x4576_11EB_4002_1DF7L, 0x0977_9EEE_5203_5616L,
        /* -164 */ 0x6F23_4FDE_CCD0_2FF1L, 0x5BF2_97E3_B66B_BCEFL,
        /* -163 */ 0x58E9_0CB2_3D73_598EL, 0x165B_ACB6_2B89_63F3L,
        /* -162 */ 0x4720_D6F4_FDF5_E13EL, 0x4516_23C4_EFA1_1CC2L,
        /* -161 */ 0x71CE_24BB_2FEF_CECAL, 0x3B56_9FA1_7F68_2E03L,
        /* -160 */ 0x5B0B_5095_BFF3_0BD5L, 0x15DE_E61A_CC53_5803L,
        /* -159 */ 0x48D5_DA11_665C_0977L, 0x2B18_B815_7042_ACCFL,
        /* -158 */ 0x7489_5CE8_A3C6_758BL, 0x5E8D_F355_806A_AE18L,
        /* -157 */ 0x5D3A_B0BA_1C9E_C46FL, 0x653E_5C44_66BB_BE7AL,
        /* -156 */ 0x4A95_5A2E_7D4B_D059L, 0x3765_169D_1EFC_9861L,
        /* -155 */ 0x7755_5D17_2EDF_B3C2L, 0x256E_8A94_FE60_F3CFL,
        /* -154 */ 0x5F77_7DAC_257F_C301L, 0x6ABE_D543_FEB3_F63FL,
        /* -153 */ 0x4C5F_97BC_EACC_9C01L, 0x3BCB_DDCF_FEF6_5E99L,
        /* -152 */ 0x7A32_8C61_77AD_C668L, 0x5FAC_9619_97F0_975BL,
        /* -151 */ 0x61C2_09E7_92F1_6B86L, 0x7FBD_44E1_465A_12AFL,
        /* -150 */ 0x4E34_D4B9_425A_BC6BL, 0x7FCA_9D81_0514_DBBFL,
        /* -149 */ 0x7D21_545B_9D5D_FA46L, 0x32DD_C8CE_6E87_C5FFL,
        /* -148 */ 0x641A_A9E2_E44B_2E9EL, 0x5BE4_A0A5_2539_6B32L,
        /* -147 */ 0x5015_54B5_836F_587EL, 0x7CB6_E6EA_842D_EF5CL,
        /* -146 */ 0x4011_1091_35F2_AD32L, 0x3092_5255_368B_25E3L,
        /* -145 */ 0x6681_B41B_8984_4850L, 0x4DB6_EA21_F0DE_A304L,
        /* -144 */ 0x5201_5CE2_D469_D373L, 0x57C5_881B_2718_826AL,
        /* -143 */ 0x419A_B0B5_76BB_0F8FL, 0x5FD1_39AF_527A_01EFL,
        /* -142 */ 0x68F7_8122_5791_B27FL, 0x4C81_F5E5_50C3_364AL,
        /* -141 */ 0x53F9_341B_7941_5B99L, 0x239B_2B1D_DA35_C508L,
        /* -140 */ 0x432D_C349_2DCD_E2E1L, 0x02E2_88E4_AE91_6A6DL,
        /* -139 */ 0x6B7C_6BA8_4949_6B01L, 0x516A_74A1_174F_10AEL,
        /* -138 */ 0x55FD_22ED_076D_EF34L, 0x4121_F6E7_45D8_DA25L,
        /* -137 */ 0x44CA_8257_3924_BF5DL, 0x1A81_9252_9E47_14EBL,
        /* -136 */ 0x6E10_D08B_8EA1_322EL, 0x5D9C_1D50_FD3E_87DDL,
        /* -135 */ 0x580D_73A2_D880_F4F2L, 0x17B0_1773_FDCB_9FE4L,
        /* -134 */ 0x4671_294F_139A_5D8EL, 0x4626_7929_97D6_1984L,
        /* -133 */ 0x70B5_0EE4_EC2A_2F4AL, 0x3D0A_5B75_BFBC_F59FL,
        /* -132 */ 0x5A2A_7250_BCEE_8C3BL, 0x4A6E_AF91_6630_C47FL,
        /* -131 */ 0x4821_F50D_63F2_09C9L, 0x21F2_260D_EB5A_36CCL,
        /* -130 */ 0x7369_8815_6CB6_760EL, 0x6983_7016_455D_247AL,
        /* -129 */ 0x5C54_6CDD_F091_F80BL, 0x6E02_C011_D117_5062L,
        /* -128 */ 0x49DD_23E4_C074_C66FL, 0x719B_CCDB_0DAC_404EL,
        /* -127 */ 0x762E_9FD4_6721_3D7FL, 0x68F9_47C4_E2AD_33B0L,
        /* -126 */ 0x5E8B_B310_5280_FDFFL, 0x6D94_396A_4EF0_F627L,
        /* -125 */ 0x4BA2_F5A6_A867_3199L, 0x3E10_2DEE_A58D_91B9L,
        /* -124 */ 0x7904_BC3D_DA3E_B5C2L, 0x3019_E317_6F48_E927L,
        /* -123 */ 0x60D0_9697_E1CB_C49BL, 0x4014_B5AC_5907_20ECL,
        /* -122 */ 0x4D73_ABAC_B4A3_03AFL, 0x4CDD_5E23_7A6C_1A57L,
        /* -121 */ 0x7BEC_45E1_2104_D2B2L, 0x47C8_969F_2A46_908AL,
        /* -120 */ 0x6323_6B1A_80D0_A88EL, 0x6CA0_787F_5505_406FL,
        /* -119 */ 0x4F4F_88E2_00A6_ED3FL, 0x0A19_F9FF_7737_66BFL,
        /* -118 */ 0x7EE5_A7D0_010B_1531L, 0x5CF6_5CCB_F1F2_3DFEL,
        /* -117 */ 0x6584_8640_00D5_AA8EL, 0x172B_7D6F_F4C1_CB32L,
        /* -116 */ 0x5136_D1CC_CD77_BBA4L, 0x78EF_978C_C3CE_3C28L,
        /* -115 */ 0x40F8_A7D7_0AC6_2FB7L, 0x13F2_DFA3_CFD8_3020L,
        /* -114 */ 0x67F4_3FBE_77A3_7F8BL, 0x3984_9906_1959_E699L,
        /* -113 */ 0x5329_CC98_5FB5_FFA2L, 0x6136_E0D1_ADE1_8548L,
        /* -112 */ 0x4287_D6E0_4C91_994FL, 0x00F8_B3DA_F181_376DL,
        /* -111 */ 0x6A72_F166_E0E8_F54BL, 0x1B27_862B_1C01_F247L,
        /* -110 */ 0x5528_C11F_1A53_F76FL, 0x2F52_D1BC_1667_F506L,
        /* -109 */ 0x4420_9A7F_4843_2C59L, 0x0C42_4163_451F_F738L,
        /* -108 */ 0x6D00_F732_0D38_46F4L, 0x7A03_9BD2_0833_2526L,
        /* -107 */ 0x5733_F8F4_D760_38C3L, 0x7B36_1641_A028_EA85L,
        /* -106 */ 0x45C3_2D90_AC4C_FA36L, 0x2F5E_7834_8020_BB9EL,
        /* -105 */ 0x6F9E_AF4D_E07B_29F0L, 0x4BCA_59ED_99CD_F8FCL,
        /* -104 */ 0x594B_BF71_8062_87F3L, 0x563B_7B24_7B0B_2D96L,
        /* -103 */ 0x476F_CC5A_CD1B_9FF6L, 0x11C9_2F50_626F_57ACL,
        /* -102 */ 0x724C_7A2A_E1C5_CCBDL, 0x02DB_7EE7_03E5_5912L,
        /* -101 */ 0x5B70_61BB_E7D1_7097L, 0x1BE2_CBEC_031D_E0DCL,
        /* -100 */ 0x4926_B496_530D_F3ACL, 0x164F_0989_9C17_E716L,
        /*  -99 */ 0x750A_BA8A_1E7C_B913L, 0x3D4B_4275_C68C_A4F0L,
        /*  -98 */ 0x5DA2_2ED4_E530_940FL, 0x4AA2_9B91_6BA3_B726L,
        /*  -97 */ 0x4AE8_2577_1DC0_7672L, 0x6EE8_7C74_561C_9285L,
        /*  -96 */ 0x77D9_D58B_62CD_8A51L, 0x3173_FA53_BCFA_8408L,
        /*  -95 */ 0x5FE1_77A2_B571_3B74L, 0x278F_FB76_30C8_69A0L,
        /*  -94 */ 0x4CB4_5FB5_5DF4_2F90L, 0x1FA6_62C4_F3D3_87B3L,
        /*  -93 */ 0x7ABA_32BB_C986_B280L, 0x32A3_D13B_1FB8_D91FL,
        /*  -92 */ 0x622E_8EFC_A138_8ECDL, 0x0EE9_742F_4C93_E0E6L,
        /*  -91 */ 0x4E8B_A596_E760_723DL, 0x58BA_C359_0A0F_E71EL,
        /*  -90 */ 0x7DAC_3C24_A567_1D2FL, 0x412A_D228_1019_71C9L,
        /*  -89 */ 0x6489_C9B6_EAB8_E426L, 0x00EF_0E86_7347_8E3BL,
        /*  -88 */ 0x506E_3AF8_BBC7_1CEBL, 0x1A58_D86B_8F6C_71C9L,
        /*  -87 */ 0x4058_2F2D_6305_B0BCL, 0x1513_E056_0C56_C16EL,
        /*  -86 */ 0x66F3_7EAF_04D5_E793L, 0x3B53_0089_AD57_9BE2L,
        /*  -85 */ 0x525C_6558_D0AB_1FA9L, 0x15DC_006E_2446_164FL,
        /*  -84 */ 0x41E3_8447_0D55_B2EDL, 0x5E49_99F1_B69E_783FL,
        /*  -83 */ 0x696C_06D8_1555_EB15L, 0x7D42_8FE9_2430_C065L,
        /*  -82 */ 0x5456_6BE0_1111_88DEL, 0x3102_0CBA_835A_3384L,
        /*  -81 */ 0x4378_564C_DA74_6D7EL, 0x5A68_0A2E_CF7B_5C69L,
        /*  -80 */ 0x6BF3_BD47_C3ED_7BFDL, 0x770C_DD17_B25E_FA42L,
        /*  -79 */ 0x565C_976C_9CBD_FCCBL, 0x1270_B0DF_C1E5_9502L,
        /*  -78 */ 0x4516_DF8A_16FE_63D5L, 0x5B8D_5A4C_9B1E_10CEL,
        /*  -77 */ 0x6E8A_FF43_57FD_6C89L, 0x127B_C3AD_C4FC_E7B0L,
        /*  -76 */ 0x586F_329C_4664_56D4L, 0x0EC9_6957_D0CA_52F3L,
        /*  -75 */ 0x46BF_5BB0_3850_4576L, 0x3F07_8779_73D5_0F29L,
        /*  -74 */ 0x7132_2C4D_26E6_D58AL, 0x31A5_A58F_1FBB_4B75L,
        /*  -73 */ 0x5A8E_89D7_5252_446EL, 0x5AEA_EAD8_E62F_6F91L,
        /*  -72 */ 0x4872_07DF_750E_9D25L, 0x2F22_557A_51BF_8C74L,
        /*  -71 */ 0x73E9_A632_54E4_2EA2L, 0x1836_EF2A_1C65_AD86L,
        /*  -70 */ 0x5CBA_EB5B_771C_F21BL, 0x2CF8_BF54_E384_8AD2L,
        /*  -69 */ 0x4A2F_22AF_927D_8E7CL, 0x23FA_32AA_4F9D_3BDBL,
        /*  -68 */ 0x76B1_D118_EA62_7D93L, 0x5329_EAAA_18FB_92F8L,
        /*  -67 */ 0x5EF4_A747_21E8_6476L, 0x0F54_BBBB_472F_A8C6L,
        /*  -66 */ 0x4BF6_EC38_E7ED_1D2BL, 0x25DD_62FC_38F2_ED6CL,
        /*  -65 */ 0x798B_138E_3FE1_C845L, 0x22FB_D193_8E51_7BDFL,
        /*  -64 */ 0x613C_0FA4_FFE7_D36AL, 0x4F2F_DADC_71DA_C97FL,
        /*  -63 */ 0x4DC9_A61D_9986_42BBL, 0x58F3_157D_27E2_3ACCL,
        /*  -62 */ 0x7C75_D695_C270_6AC5L, 0x74B8_2261_D969_F7ADL,
        /*  -61 */ 0x6391_7877_CEC0_556BL, 0x1093_4EB4_ADEE_5FBEL,
        /*  -60 */ 0x4FA7_9393_0BCD_1122L, 0x4075_D890_8B25_1965L,
        /*  -59 */ 0x7F72_85B8_12E1_B504L, 0x00BC_8DB4_11D4_F56EL,
        /*  -58 */ 0x65F5_37C6_7581_5D9CL, 0x66FD_3E29_A7DD_9125L,
        /*  -57 */ 0x5190_F96B_9134_4AE3L, 0x6BFD_CB54_864A_DA84L,
        /*  -56 */ 0x4140_C789_40F6_A24FL, 0x6FFE_3C43_9EA2_486AL,
        /*  -55 */ 0x6867_A5A8_67F1_03B2L, 0x7FFD_2D38_FDD0_73DCL,
        /*  -54 */ 0x5386_1E20_5327_3628L, 0x6664_242D_97D9_F64AL,
        /*  -53 */ 0x42D1_B1B3_75B8_F820L, 0x51E9_B68A_DFE1_91D5L,
        /*  -52 */ 0x6AE9_1C52_55F4_C034L, 0x1CA9_2411_6635_B621L,
        /*  -51 */ 0x5587_49DB_77F7_0029L, 0x63BA_8341_1E91_5E81L,
        /*  -50 */ 0x446C_3B15_F992_6687L, 0x6962_029A_7EDA_B201L,
        /*  -49 */ 0x6D79_F823_28EA_3DA6L, 0x0F03_375D_97C4_5001L,
        /*  -48 */ 0x5794_C682_8721_CAEBL, 0x259C_2C4A_DFD0_4001L,
        /*  -47 */ 0x4610_9ECE_D281_6F22L, 0x5149_BD08_B30D_0001L,
        /*  -46 */ 0x701A_97B1_50CF_1837L, 0x3542_C80D_EB48_0001L,
        /*  -45 */ 0x59AE_DFC1_0D72_79C5L, 0x7768_A00B_22A0_0001L,
        /*  -44 */ 0x47BF_1967_3DF5_2E37L, 0x7920_8008_E880_0001L,
        /*  -43 */ 0x72CB_5BD8_6321_E38CL, 0x5B67_3341_7400_0001L,
        /*  -42 */ 0x5BD5_E313_8281_82D6L, 0x7C52_8F67_9000_0001L,
        /*  -41 */ 0x4977_E8DC_6867_9BDFL, 0x16A8_72B9_4000_0001L,
        /*  -40 */ 0x758C_A7C7_0D72_92FEL, 0x5773_EAC2_0000_0001L,
        /*  -39 */ 0x5E0A_1FD2_7128_7598L, 0x45F6_5568_0000_0001L,
        /*  -38 */ 0x4B3B_4CA8_5A86_C47AL, 0x04C5_1120_0000_0001L,
        /*  -37 */ 0x785E_E10D_5DA4_6D90L, 0x07A1_B500_0000_0001L,
        /*  -36 */ 0x604B_E73D_E483_8AD9L, 0x52E7_C400_0000_0001L,
        /*  -35 */ 0x4D09_85CB_1D36_08AEL, 0x0F1F_D000_0000_0001L,
        /*  -34 */ 0x7B42_6FAB_61F0_0DE3L, 0x31CC_8000_0000_0001L,
        /*  -33 */ 0x629B_8C89_1B26_7182L, 0x5B0A_0000_0000_0001L,
        /*  -32 */ 0x4EE2_D6D4_15B8_5ACEL, 0x7C08_0000_0000_0001L,
        /*  -31 */ 0x7E37_BE20_22C0_914BL, 0x1340_0000_0000_0001L,
        /*  -30 */ 0x64F9_64E6_8233_A76FL, 0x2900_0000_0000_0001L,
        /*  -29 */ 0x50C7_83EB_9B5C_85F2L, 0x5400_0000_0000_0001L,
        /*  -28 */ 0x409F_9CBC_7C4A_04C2L, 0x1000_0000_0000_0001L,
        /*  -27 */ 0x6765_C793_FA10_079DL, 0x0000_0000_0000_0001L,
        /*  -26 */ 0x52B7_D2DC_C80C_D2E4L, 0x0000_0000_0000_0001L,
        /*  -25 */ 0x422C_A8B0_A00A_4250L, 0x0000_0000_0000_0001L,
        /*  -24 */ 0x69E1_0DE7_6676_D080L, 0x0000_0000_0000_0001L,
        /*  -23 */ 0x54B4_0B1F_852B_DA00L, 0x0000_0000_0000_0001L,
        /*  -22 */ 0x43C3_3C19_3756_4800L, 0x0000_0000_0000_0001L,
        /*  -21 */ 0x6C6B_935B_8BBD_4000L, 0x0000_0000_0000_0001L,
        /*  -20 */ 0x56BC_75E2_D631_0000L, 0x0000_0000_0000_0001L,
        /*  -19 */ 0x4563_9182_44F4_0000L, 0x0000_0000_0000_0001L,
        /*  -18 */ 0x6F05_B59D_3B20_0000L, 0x0000_0000_0000_0001L,
        /*  -17 */ 0x58D1_5E17_6280_0000L, 0x0000_0000_0000_0001L,
        /*  -16 */ 0x470D_E4DF_8200_0000L, 0x0000_0000_0000_0001L,
        /*  -15 */ 0x71AF_D498_D000_0000L, 0x0000_0000_0000_0001L,
        /*  -14 */ 0x5AF3_107A_4000_0000L, 0x0000_0000_0000_0001L,
        /*  -13 */ 0x48C2_7395_0000_0000L, 0x0000_0000_0000_0001L,
        /*  -12 */ 0x746A_5288_0000_0000L, 0x0000_0000_0000_0001L,
        /*  -11 */ 0x5D21_DBA0_0000_0000L, 0x0000_0000_0000_0001L,
        /*  -10 */ 0x4A81_7C80_0000_0000L, 0x0000_0000_0000_0001L,
        /*   -9 */ 0x7735_9400_0000_0000L, 0x0000_0000_0000_0001L,
        /*   -8 */ 0x5F5E_1000_0000_0000L, 0x0000_0000_0000_0001L,
        /*   -7 */ 0x4C4B_4000_0000_0000L, 0x0000_0000_0000_0001L,
        /*   -6 */ 0x7A12_0000_0000_0000L, 0x0000_0000_0000_0001L,
        /*   -5 */ 0x61A8_0000_0000_0000L, 0x0000_0000_0000_0001L,
        /*   -4 */ 0x4E20_0000_0000_0000L, 0x0000_0000_0000_0001L,
        /*   -3 */ 0x7D00_0000_0000_0000L, 0x0000_0000_0000_0001L,
        /*   -2 */ 0x6400_0000_0000_0000L, 0x0000_0000_0000_0001L,
        /*   -1 */ 0x5000_0000_0000_0000L, 0x0000_0000_0000_0001L,
        /*    0 */ 0x4000_0000_0000_0000L, 0x0000_0000_0000_0001L,
        /*    1 */ 0x6666_6666_6666_6666L, 0x3333_3333_3333_3334L,
        /*    2 */ 0x51EB_851E_B851_EB85L, 0x0F5C_28F5_C28F_5C29L,
        /*    3 */ 0x4189_374B_C6A7_EF9DL, 0x5916_872B_020C_49BBL,
        /*    4 */ 0x68DB_8BAC_710C_B295L, 0x74F0_D844_D013_A92BL,
        /*    5 */ 0x53E2_D623_8DA3_C211L, 0x43F3_E037_0CDC_8755L,
        /*    6 */ 0x431B_DE82_D7B6_34DAL, 0x698F_E692_70B0_6C44L,
        /*    7 */ 0x6B5F_CA6A_F2BD_215EL, 0x0F4C_A41D_811A_46D4L,
        /*    8 */ 0x55E6_3B88_C230_E77EL, 0x3F70_834A_CDAE_9F10L,
        /*    9 */ 0x44B8_2FA0_9B5A_52CBL, 0x4C5A_02A2_3E25_4C0DL,
        /*   10 */ 0x6DF3_7F67_5EF6_EADFL, 0x2D5C_D103_96A2_1347L,
        /*   11 */ 0x57F5_FF85_E592_557FL, 0x3DE3_DA69_454E_75D3L,
        /*   12 */ 0x465E_6604_B7A8_4465L, 0x7E4F_E1ED_D10B_9175L,
        /*   13 */ 0x7097_09A1_25DA_0709L, 0x4A19_697C_81AC_1BEFL,
        /*   14 */ 0x5A12_6E1A_84AE_6C07L, 0x54E1_2130_67BC_E326L,
        /*   15 */ 0x480E_BE7B_9D58_566CL, 0x43E7_4DC0_52FD_8285L,
        /*   16 */ 0x734A_CA5F_6226_F0ADL, 0x530B_AF9A_1E62_6A6DL,
        /*   17 */ 0x5C3B_D519_1B52_5A24L, 0x426F_BFAE_7EB5_21F1L,
        /*   18 */ 0x49C9_7747_490E_AE83L, 0x4EBF_CC8B_9890_E7F4L,
        /*   19 */ 0x760F_253E_DB4A_B0D2L, 0x4ACC_7A78_F41B_0CBAL,
        /*   20 */ 0x5E72_8432_4908_8D75L, 0x223D_2EC7_29AF_3D62L,
        /*   21 */ 0x4B8E_D028_3A6D_3DF7L, 0x34FD_BF05_BAF2_9781L,
        /*   22 */ 0x78E4_8040_5D7B_9658L, 0x54C9_31A2_C4B7_58CFL,
        /*   23 */ 0x60B6_CD00_4AC9_4513L, 0x5D6D_C14F_03C5_E0A5L,
        /*   24 */ 0x4D5F_0A66_A23A_9DA9L, 0x3124_9AA5_9C9E_4D51L,
        /*   25 */ 0x7BCB_43D7_69F7_62A8L, 0x4EA0_F76F_60FD_4882L,
        /*   26 */ 0x6309_0312_BB2C_4EEDL, 0x254D_92BF_80CA_A068L,
        /*   27 */ 0x4F3A_68DB_C8F0_3F24L, 0x1DD7_A899_33D5_4D20L,
        /*   28 */ 0x7EC3_DAF9_4180_6506L, 0x62F2_A75B_8622_1500L,
        /*   29 */ 0x6569_7BFA_9ACD_1D9FL, 0x025B_B916_04E8_10CDL,
        /*   30 */ 0x5121_2FFB_AF0A_7E18L, 0x6849_60DE_6A53_40A4L,
        /*   31 */ 0x40E7_5996_25A1_FE7AL, 0x203A_B3E5_21DC_33B6L,
        /*   32 */ 0x67D8_8F56_A29C_CA5DL, 0x19F7_863B_6960_52BDL,
        /*   33 */ 0x5313_A5DE_E87D_6EB0L, 0x7B2C_6B62_BAB3_7564L,
        /*   34 */ 0x4276_1E4B_ED31_255AL, 0x2F56_BC4E_FBC2_C450L,
        /*   35 */ 0x6A56_96DF_E1E8_3BC3L, 0x6557_93B1_92D1_3A1AL,
        /*   36 */ 0x5512_124C_B4B9_C969L, 0x3779_42F4_7574_2E7BL,
        /*   37 */ 0x440E_750A_2A2E_3ABAL, 0x5F94_3590_5DF6_8B96L,
        /*   38 */ 0x6CE3_EE76_A9E3_912AL, 0x65B9_EF4D_6324_1289L,
        /*   39 */ 0x571C_BEC5_54B6_0DBBL, 0x6AFB_25D7_8283_4207L,
        /*   40 */ 0x45B0_989D_DD5E_7163L, 0x08C8_EB12_CECF_6806L,
        /*   41 */ 0x6F80_F42F_C897_1BD1L, 0x5ADB_11B7_B14B_D9A3L,
        /*   42 */ 0x5933_F68C_A078_E30EL, 0x157C_0E2C_8DD6_47B5L,
        /*   43 */ 0x475C_C53D_4D2D_8271L, 0x5DFC_D823_A4AB_6C91L,
        /*   44 */ 0x722E_0862_1515_9D82L, 0x632E_269F_6DDF_141BL,
        /*   45 */ 0x5B58_06B4_DDAA_E468L, 0x4F58_1EE5_F17F_4349L,
        /*   46 */ 0x4913_3890_B155_8386L, 0x72AC_E584_C132_9C3BL,
        /*   47 */ 0x74EB_8DB4_4EEF_38D7L, 0x6AAE_3C07_9B84_2D2AL,
        /*   48 */ 0x5D89_3E29_D8BF_60ACL, 0x5558_3006_1603_5755L,
        /*   49 */ 0x4AD4_31BB_13CC_4D56L, 0x7779_C004_DE69_12ABL,
        /*   50 */ 0x77B9_E92B_52E0_7BBEL, 0x258F_99A1_63DB_5111L,
        /*   51 */ 0x5FC7_EDBC_424D_2FCBL, 0x37A6_1481_1CAF_740DL,
        /*   52 */ 0x4C9F_F163_683D_BFD5L, 0x7951_AA00_E3BF_900BL,
        /*   53 */ 0x7A99_8238_A6C9_32EFL, 0x754F_7667_D2CC_19ABL,
        /*   54 */ 0x6214_682D_523A_8F26L, 0x2AA5_F853_0F09_AE22L,
        /*   55 */ 0x4E76_B9BD_DB62_0C1EL, 0x5551_9375_A5A1_581BL,
        /*   56 */ 0x7D8A_C2C9_5F03_4697L, 0x3BB5_B8BC_3C35_59C5L,
        /*   57 */ 0x646F_023A_B269_0545L, 0x7C91_6096_9691_149EL,
        /*   58 */ 0x5058_CE95_5B87_376BL, 0x16DA_B3AB_ABA7_43B2L,
        /*   59 */ 0x4047_0BAA_AF9F_5F88L, 0x78AE_F622_EFB9_02F5L,
        /*   60 */ 0x66D8_12AA_B298_98DBL, 0x0DE4_BD04_B2C1_9E54L,
        /*   61 */ 0x5246_7555_5BAD_4715L, 0x57EA_30D0_8F01_4B76L,
        /*   62 */ 0x41D1_F777_7C8A_9F44L, 0x4654_F3DA_0C01_092CL,
        /*   63 */ 0x694F_F258_C744_3207L, 0x23BB_1FC3_4668_0EACL,
        /*   64 */ 0x543F_F513_D29C_F4D2L, 0x4FC8_E635_D1EC_D88AL,
        /*   65 */ 0x4366_5DA9_754A_5D75L, 0x263A_51C4_A7F0_AD3BL,
        /*   66 */ 0x6BD6_FC42_5543_C8BBL, 0x56C3_B607_731A_AEC4L,
        /*   67 */ 0x5645_969B_7769_6D62L, 0x789C_919F_8F48_8BD0L,
        /*   68 */ 0x4504_787C_5F87_8AB5L, 0x46E3_A7B2_D906_D640L,
        /*   69 */ 0x6E6D_8D93_CC0C_1122L, 0x3E39_0C51_5B3E_239AL,
        /*   70 */ 0x5857_A476_3CD6_741BL, 0x4B60_D6A7_7C31_B615L,
        /*   71 */ 0x46AC_8391_CA45_29AFL, 0x55E7_121F_968E_2B44L,
        /*   72 */ 0x7114_05B6_106E_A919L, 0x0971_B698_F0E3_786DL,
        /*   73 */ 0x5A76_6AF8_0D25_5414L, 0x078E_2BAD_8D82_C6BDL,
        /*   74 */ 0x485E_BBF9_A41D_DCDCL, 0x6C71_BC8A_D79B_D231L,
        /*   75 */ 0x73CA_C65C_39C9_6161L, 0x2D82_C744_8C2C_8382L,
        /*   76 */ 0x5CA2_3849_C7D4_4DE7L, 0x3E02_3903_A356_CF9BL,
        /*   77 */ 0x4A1B_603B_0643_7185L, 0x7E68_2D9C_82AB_D949L,
        /*   78 */ 0x7692_3391_A39F_1C09L, 0x4A40_48FA_6AAC_8EDBL,
        /*   79 */ 0x5EDB_5C74_82E5_B007L, 0x5500_3A61_EEF0_7249L,
        /*   80 */ 0x4BE2_B05D_3584_8CD2L, 0x7733_61E7_F259_F507L,
        /*   81 */ 0x796A_B3C8_55A0_E151L, 0x3EB8_9CA6_508F_EE71L,
        /*   82 */ 0x6122_296D_114D_810DL, 0x7EFA_16EB_73A6_585BL,
        /*   83 */ 0x4DB4_EDF0_DAA4_673EL, 0x3261_ABEF_8FB8_46AFL,
        /*   84 */ 0x7C54_AFE7_C43A_3ECAL, 0x1D69_1318_E5F3_A44BL,
        /*   85 */ 0x6376_F31F_D02E_98A1L, 0x6454_0F47_1E5C_836FL,
        /*   86 */ 0x4F92_5C19_7358_7A1BL, 0x0376_729F_4B7D_35F3L,
        /*   87 */ 0x7F50_935B_EBC0_C35EL, 0x38BD_8432_1261_EFEBL,
        /*   88 */ 0x65DA_0F7C_BC9A_35E5L, 0x13CA_D028_0EB4_BFEFL,
        /*   89 */ 0x517B_3F96_FD48_2B1DL, 0x5CA2_4020_0BC3_CCBFL,
        /*   90 */ 0x412F_6612_6439_BC17L, 0x63B5_0019_A303_0A33L,
        /*   91 */ 0x684B_D683_D38F_9359L, 0x1F88_0029_04D1_A9EAL,
        /*   92 */ 0x536F_DECF_DC72_DC47L, 0x32D3_3354_03DA_EE55L,
        /*   93 */ 0x42BF_E573_16C2_49D2L, 0x5BDC_2910_0315_8B77L,
        /*   94 */ 0x6ACC_A251_BE03_A951L, 0x12F9_DB4C_D1BC_1258L,
        /*   95 */ 0x5570_81DA_FE69_5440L, 0x7594_AF70_A7C9_A847L,
        /*   96 */ 0x445A_017B_FEBA_A9CDL, 0x4476_F2C0_863A_ED06L,
        /*   97 */ 0x6D5C_CF2C_CAC4_42E2L, 0x3A57_EACD_A391_7B3CL,
        /*   98 */ 0x577D_728A_3BD0_3581L, 0x7B79_88A4_82DA_C8FDL,
        /*   99 */ 0x45FD_F53B_630C_F79BL, 0x15FA_D3B6_CF15_6D97L,
        /*  100 */ 0x6FFC_BB92_3814_BF5EL, 0x565E_1F8A_E4EF_15BEL,
        /*  101 */ 0x5996_FC74_F9AA_32B2L, 0x11E4_E608_B725_AAFFL,
        /*  102 */ 0x47AB_FD2A_6154_F55BL, 0x27EA_51A0_9284_88CCL,
        /*  103 */ 0x72AC_C843_CEEE_555EL, 0x7310_829A_8407_4146L,
        /*  104 */ 0x5BBD_6D03_0BF1_DDE5L, 0x4273_9BAE_D005_CDD2L,
        /*  105 */ 0x4964_5735_A327_E4B7L, 0x4EC2_E2F2_4004_A4A8L,
        /*  106 */ 0x756D_5855_D1D9_6DF2L, 0x4AD1_6B1D_333A_A10CL,
        /*  107 */ 0x5DF1_1377_DB14_57F5L, 0x2241_227D_C295_4DA3L,
        /*  108 */ 0x4B27_42C6_48DD_132AL, 0x4E9A_81FE_3544_3E1CL,
        /*  109 */ 0x783E_D13D_4161_B844L, 0x175D_9CC9_EED3_9694L,
        /*  110 */ 0x6032_40FD_CDE7_C69CL, 0x7917_B0A1_8BDC_7876L,
        /*  111 */ 0x4CF5_00CB_0B1F_D217L, 0x1412_F3B4_6FE3_9392L,
        /*  112 */ 0x7B21_9ADE_7832_E9BEL, 0x5351_85ED_7FD2_85B6L,
        /*  113 */ 0x6281_48B1_F9C2_5498L, 0x42A7_9E57_9975_37C5L,
        /*  114 */ 0x4ECD_D3C1_949B_76E0L, 0x3552_E512_E12A_9304L,
        /*  115 */ 0x7E16_1F9C_20F8_BE33L, 0x6EEB_081E_3510_EB39L,
        /*  116 */ 0x64DE_7FB0_1A60_9829L, 0x3F22_6CE4_F740_BC2EL,
        /*  117 */ 0x50B1_FFC0_151A_1354L, 0x3281_F0B7_2C33_C9BEL,
        /*  118 */ 0x408E_6633_4414_DC43L, 0x4201_8D5F_568F_D498L,
        /*  119 */ 0x674A_3D1E_D354_939FL, 0x1CCF_4898_8A7F_BA8DL,
        /*  120 */ 0x52A1_CA7F_0F76_DC7FL, 0x30A5_D3AD_3B99_620BL,
        /*  121 */ 0x421B_0865_A5F8_B065L, 0x73B7_DC8A_9614_4E6FL,
        /*  122 */ 0x69C4_DA3C_3CC1_1A3CL, 0x52BF_C744_2353_B0B1L,
        /*  123 */ 0x549D_7B63_63CD_AE96L, 0x7566_3903_4F76_26F4L,
        /*  124 */ 0x43B1_2F82_B63E_2545L, 0x4451_C735_D92B_525DL,
        /*  125 */ 0x6C4E_B26A_BD30_3BA2L, 0x3A1C_71EF_C1DE_EA2EL,
        /*  126 */ 0x56A5_5B88_9759_C94EL, 0x61B0_5B26_34B2_54F2L,
        /*  127 */ 0x4551_1606_DF7B_0772L, 0x1AF3_7C1E_908E_AA5BL,
        /*  128 */ 0x6EE8_233E_325E_7250L, 0x2B1F_2CFD_B417_76F8L,
        /*  129 */ 0x58B9_B5CB_5B7E_C1D9L, 0x6F4C_23FE_29AC_5F2DL,
        /*  130 */ 0x46FA_F7D5_E2CB_CE47L, 0x72A3_4FFE_87BD_18F1L,
        /*  131 */ 0x7191_8C89_6ADF_B073L, 0x0438_7FFD_A5FB_5B1BL,
        /*  132 */ 0x5ADA_D6D4_557F_C05CL, 0x0360_6664_84C9_15AFL,
        /*  133 */ 0x48AF_1243_7799_66B0L, 0x02B3_851D_3707_448CL,
        /*  134 */ 0x744B_506B_F28F_0AB3L, 0x1DEC_082E_BE72_0746L,
        /*  135 */ 0x5D09_0D23_2872_6EF5L, 0x64BC_D358_985B_3905L,
        /*  136 */ 0x4A6D_A41C_205B_8BF7L, 0x6A30_A913_AD15_C738L,
        /*  137 */ 0x7715_D360_33C5_ACBFL, 0x5D1A_A81F_7B56_0B8CL,
        /*  138 */ 0x5F44_A919_C304_8A32L, 0x7DAE_ECE5_FC44_D609L,
        /*  139 */ 0x4C36_EDAE_359D_3B5BL, 0x7E25_8A51_969D_7808L,
        /*  140 */ 0x79F1_7C49_EF61_F893L, 0x16A2_76E8_F0FB_F33FL,
        /*  141 */ 0x618D_FD07_F2B4_C6DCL, 0x121B_9253_F3FC_C299L,
        /*  142 */ 0x4E0B_30D3_2890_9F16L, 0x41AF_A843_2997_0214L,
        /*  143 */ 0x7CDE_B485_0DB4_31BDL, 0x4F7F_739E_A8F1_9CEDL,
        /*  144 */ 0x63E5_5D37_3E29_C164L, 0x3F99_294B_BA5A_E3F1L,
        /*  145 */ 0x4FEA_B0F8_FE87_CDE9L, 0x7FAD_BAA2_FB7B_E98DL,
        /*  146 */ 0x7FDD_E7F4_CA72_E30FL, 0x7F7C_5DD1_925F_DC15L,
        /*  147 */ 0x664B_1FF7_085B_E8D9L, 0x4C63_7E41_41E6_49ABL,
        /*  148 */ 0x51D5_B32C_06AF_ED7AL, 0x704F_9834_34B8_3AEFL,
        /*  149 */ 0x4177_C289_9EF3_2462L, 0x26A6_135C_F6F9_C8BFL,
        /*  150 */ 0x68BF_9DA8_FE51_D3D0L, 0x3DD6_8561_8B29_4132L,
        /*  151 */ 0x53CC_7E20_CB74_A973L, 0x4B12_044E_08ED_CDC2L,
        /*  152 */ 0x4309_FE80_A2C3_BAC2L, 0x6F41_9D0B_3A57_D7CEL,
        /*  153 */ 0x6B43_30CD_D139_2AD1L, 0x3202_94DE_C3BF_BFB0L,
        /*  154 */ 0x55CF_5A3E_40FA_88A7L, 0x419B_AA4B_CFCC_995AL,
        /*  155 */ 0x44A5_E1CB_672E_D3B9L, 0x1AE2_EEA3_0CA3_ADE1L,
        /*  156 */ 0x6DD6_3612_3EB1_52C1L, 0x77D1_7DD1_ADD2_AFCFL,
        /*  157 */ 0x57DE_91A8_3227_7567L, 0x7974_64A7_BE42_263FL,
        /*  158 */ 0x464B_A7B9_C1B9_2AB9L, 0x4790_5086_31CE_84FFL,
        /*  159 */ 0x7079_0C5C_6928_445CL, 0x0C1A_1A70_4FB0_D4CCL,
        /*  160 */ 0x59FA_7049_EDB9_D049L, 0x567B_4859_D95A_43D6L,
        /*  161 */ 0x47FB_8D07_F161_736EL, 0x11FC_39E1_7AAE_9CABL,
        /*  162 */ 0x732C_14D9_8235_857DL, 0x032D_2968_C44A_9445L,
        /*  163 */ 0x5C23_43E1_34F7_9DFDL, 0x4F57_5453_D03B_A9D1L,
        /*  164 */ 0x49B5_CFE7_5D92_E4CAL, 0x72AC_4376_402F_BB0EL,
        /*  165 */ 0x75EF_B30B_C8EB_07ABL, 0x0446_D256_CD19_2B49L,
        /*  166 */ 0x5E59_5C09_6D88_D2EFL, 0x1D05_7512_3DAD_BC3AL,
        /*  167 */ 0x4B7A_B007_8AD3_DBF2L, 0x4A6A_C40E_97BE_302FL,
        /*  168 */ 0x78C4_4CD8_DE1F_C650L, 0x7711_39B0_F2C9_E6B1L,
        /*  169 */ 0x609D_0A47_1819_6B73L, 0x78DA_948D_8F07_EBC1L,
        /*  170 */ 0x4D4A_6E9F_467A_BC5CL, 0x60AE_DD3E_0C06_5634L,
        /*  171 */ 0x7BAA_4A98_70C4_6094L, 0x344A_FB96_79A3_BD20L,
        /*  172 */ 0x62EE_A213_8D69_E6DDL, 0x103B_FC78_614F_CA80L,
        /*  173 */ 0x4F25_4E76_0ABB_1F17L, 0x2696_6393_810C_A200L,
        /*  174 */ 0x7EA2_1723_445E_9825L, 0x2423_D285_9B47_6999L,
        /*  175 */ 0x654E_78E9_037E_E01DL, 0x69B6_4204_7C39_2148L,
        /*  176 */ 0x510B_93ED_9C65_8017L, 0x6E2B_6803_9694_1AA0L,
        /*  177 */ 0x40D6_0FF1_49EA_CCDFL, 0x71BC_5336_1210_154DL,
        /*  178 */ 0x67BC_E64E_DCAA_E166L, 0x1C60_8523_5019_BBAEL,
        /*  179 */ 0x52FD_850B_E3BB_E784L, 0x7D1A_041C_4014_9625L,
        /*  180 */ 0x4264_6A6F_E963_1F9DL, 0x4A7B_367D_0010_781DL,
        /*  181 */ 0x6A3A_43E6_4238_3295L, 0x5D91_F0C8_001A_59C8L,
        /*  182 */ 0x54FB_6985_01C6_8EDEL, 0x17A7_F3D3_3348_47D4L,
        /*  183 */ 0x43FC_546A_67D2_0BE4L, 0x7953_2975_C2A0_3976L,
        /*  184 */ 0x6CC6_ED77_0C83_463BL, 0x0EEB_7589_3766_C256L,
        /*  185 */ 0x5705_8AC5_A39C_382FL, 0x2589_2AD4_2C52_3512L,
        /*  186 */ 0x459E_089E_1C7C_F9BFL, 0x37A0_EF10_2374_F742L,
        /*  187 */ 0x6F63_40FC_FA61_8F98L, 0x5901_7E80_38BB_2536L,
        /*  188 */ 0x591C_33FD_951A_D946L, 0x7A67_9866_93C8_EA91L,
        /*  189 */ 0x4749_C331_4415_7A9FL, 0x151F_AD1E_DCA0_BBA8L,
        /*  190 */ 0x720F_9EB5_39BB_F765L, 0x0832_AE97_C767_92A5L,
        /*  191 */ 0x5B3F_B22A_9496_5F84L, 0x068E_F213_05EC_7551L,
        /*  192 */ 0x48FF_C1BB_AA11_E603L, 0x1ED8_C1A8_D189_F774L,
        /*  193 */ 0x74CC_692C_434F_D66BL, 0x4AF4_690E_1C0F_F253L,
        /*  194 */ 0x5D70_5423_690C_AB89L, 0x225D_20D8_1673_2843L,
        /*  195 */ 0x4AC0_434F_873D_5607L, 0x3517_4D79_AB8F_5369L,
        /*  196 */ 0x779A_054C_0B95_5672L, 0x21BE_E25C_45B2_1F0EL,
        /*  197 */ 0x5FAE_6AA3_3C77_785BL, 0x3498_B516_9E28_18D8L,
        /*  198 */ 0x4C8B_8882_96C5_F9E2L, 0x5D46_F745_4B53_4713L,
        /*  199 */ 0x7A78_DA6A_8AD6_5C9DL, 0x7BA4_BED5_4552_0B52L,
        /*  200 */ 0x61FA_4855_3BDE_B07EL, 0x2FB6_FF11_0441_A2A8L,
        /*  201 */ 0x4E61_D377_6318_8D31L, 0x72F8_CC0D_9D01_4EEDL,
        /*  202 */ 0x7D69_5258_9E8D_AEB6L, 0x1E5A_E015_C802_17E1L,
        /*  203 */ 0x6454_41E0_7ED7_BEF8L, 0x1848_B344_A001_ACB4L,
        /*  204 */ 0x5043_67E6_CBDF_CBF9L, 0x603A_2903_B334_8A2AL,
        /*  205 */ 0x4035_ECB8_A319_6FFBL, 0x002E_8736_28F6_D4EEL,
        /*  206 */ 0x66BC_ADF4_3828_B32BL, 0x19E4_0B89_DB24_87E3L,
        /*  207 */ 0x5230_8B29_C686_F5BCL, 0x14B6_6FA1_7C1D_3983L,
        /*  208 */ 0x41C0_6F54_9ED2_5E30L, 0x1091_F2E7_967D_C79CL,
        /*  209 */ 0x6933_E554_3150_96B3L, 0x341C_B7D8_F0C9_3F5FL,
        /*  210 */ 0x5429_8443_5AA6_DEF5L, 0x767D_5FE0_C0A0_FF80L,
        /*  211 */ 0x4354_69CF_7BB8_B25EL, 0x2B97_7FE7_0080_CC66L,
        /*  212 */ 0x6BBA_42E5_92C1_1D63L, 0x5F58_CCA4_CD9A_E0A3L,
        /*  213 */ 0x562E_9BEA_DBCD_B11CL, 0x4C47_0A1D_7148_B3B6L,
        /*  214 */ 0x44F2_1655_7CA4_8DB0L, 0x3D05_A1B1_276D_5C92L,
        /*  215 */ 0x6E50_23BB_FAA0_E2B3L, 0x7B3C_35E8_3F15_60E9L,
        /*  216 */ 0x5840_1C96_621A_4EF6L, 0x2F63_5E53_65AA_B3EDL,
        /*  217 */ 0x4699_B078_4E7B_725EL, 0x591C_4B75_EAEE_F658L,
        /*  218 */ 0x70F5_E726_E3F8_B6FDL, 0x74FA_1256_44B1_8A26L,
        /*  219 */ 0x5A5E_5285_832D_5F31L, 0x43FB_41DE_9D5A_D4EBL,
        /*  220 */ 0x484B_7537_9C24_4C27L, 0x4FFC_34B2_177B_DD89L,
        /*  221 */ 0x73AB_EEBF_603A_1372L, 0x4CC6_BAB6_8BF9_6274L,
        /*  222 */ 0x5C89_8BCC_4CFB_42C2L, 0x0A38_955E_D661_1B90L,
        /*  223 */ 0x4A07_A309_D72F_689BL, 0x21C6_DDE5_784D_AFA7L,
        /*  224 */ 0x7672_9E76_2518_A75EL, 0x693E_2FD5_8D49_190BL,
        /*  225 */ 0x5EC2_185E_8413_B918L, 0x5431_BFDE_0AA0_E0D5L,
        /*  226 */ 0x4BCE_79E5_3676_2DADL, 0x29C1_664B_3BB3_E711L,
        /*  227 */ 0x794A_5CA1_F0BD_15E2L, 0x0F9B_D6DE_C5EC_A4E8L,
        /*  228 */ 0x6108_4A1B_26FD_AB1BL, 0x2616_457F_04BD_50BAL,
        /*  229 */ 0x4DA0_3B48_EBFE_227CL, 0x1E78_3798_D097_73C8L,
        /*  230 */ 0x7C33_920E_4663_6A60L, 0x30C0_58F4_80F2_52D9L,
        /*  231 */ 0x635C_74D8_384F_884DL, 0x0D66_AD90_6728_4247L,
        /*  232 */ 0x4F7D_2A46_9372_D370L, 0x711E_F140_5286_9B6CL,
        /*  233 */ 0x7F2E_AA0A_8584_8581L, 0x34FE_4ECD_50D7_5F14L,
        /*  234 */ 0x65BE_EE6E_D136_D134L, 0x2A65_0BD7_73DF_7F43L,
        /*  235 */ 0x5165_8B8B_DA92_40F6L, 0x551D_A312_C319_329CL,
        /*  236 */ 0x411E_093C_AEDB_672BL, 0x5DB1_4F42_35AD_C217L,
        /*  237 */ 0x6830_0EC7_7E2B_D845L, 0x7C4E_E536_BC49_368AL,
        /*  238 */ 0x5359_A56C_64EF_E037L, 0x7D0B_EA92_303A_9208L,
        /*  239 */ 0x42AE_1DF0_50BF_E693L, 0x173C_BBA8_2695_41A0L,
        /*  240 */ 0x6AB0_2FE6_E799_70EBL, 0x3EC7_92A6_A422_029AL,
        /*  241 */ 0x5559_BFEB_EC7A_C0BCL, 0x3239_421E_E9B4_CEE1L,
        /*  242 */ 0x4447_CCBC_BD2F_0096L, 0x5B61_01B2_5490_A581L,
        /*  243 */ 0x6D3F_ADFA_C84B_3424L, 0x2BCE_691D_541A_A268L,
        /*  244 */ 0x5766_24C8_A03C_29B6L, 0x563E_BA7D_DCE2_1B87L,
        /*  245 */ 0x45EB_50A0_8030_215EL, 0x7832_2ECB_171B_4939L,
        /*  246 */ 0x6FDE_E767_3380_3564L, 0x59E9_E478_24F8_7527L,
        /*  247 */ 0x597F_1F85_C2CC_F783L, 0x6187_E9F9_B72D_2A86L,
        /*  248 */ 0x4798_E604_9BD7_2C69L, 0x346C_BB2E_2C24_2205L,
        /*  249 */ 0x728E_3CD4_2C8B_7A42L, 0x20AD_F849_E039_D007L,
        /*  250 */ 0x5BA4_FD76_8A09_2E9BL, 0x33BE_603B_19C7_D99FL,
        /*  251 */ 0x4950_CAC5_3B3A_8BAFL, 0x42FE_B362_7B06_47B3L,
        /*  252 */ 0x754E_113B_91F7_45E5L, 0x5197_856A_5E70_72B8L,
        /*  253 */ 0x5DD8_0DC9_4192_9E51L, 0x27AC_6ABB_7EC0_5BC6L,
        /*  254 */ 0x4B13_3E3A_9ADB_B1DAL, 0x52F0_5562_CBCD_1638L,
        /*  255 */ 0x781E_C9F7_5E2C_4FC4L, 0x1E4D_556A_DFAE_89F3L,
        /*  256 */ 0x6018_A192_B1BD_0C9CL, 0x7EA4_4455_7FBE_D4C3L,
        /*  257 */ 0x4CE0_8142_27CA_707DL, 0x4BB6_9D11_32FF_109CL,
        /*  258 */ 0x7B00_CED0_3FAA_4D95L, 0x5F8A_94E8_5198_1A93L,
        /*  259 */ 0x6267_0BD9_CC88_3E11L, 0x32D5_43ED_0E13_4875L,
        /*  260 */ 0x4EB8_D647_D6D3_64DAL, 0x5BDD_CFF0_D80F_6D2BL,
        /*  261 */ 0x7DF4_8A0C_8AEB_D491L, 0x12FC_7FE7_C018_AEABL,
        /*  262 */ 0x64C3_A1A3_A256_43A7L, 0x28C9_FFEC_99AD_5889L,
        /*  263 */ 0x509C_814F_B511_CFB9L, 0x0707_FFF0_7AF1_13A1L,
        /*  264 */ 0x407D_343F_C40E_3FC7L, 0x1F39_998D_2F27_42E7L,
        /*  265 */ 0x672E_B9FF_A016_CC71L, 0x7EC2_8F48_4B72_04A4L,
        /*  266 */ 0x528B_C7FF_B345_705BL, 0x189B_A5D3_6F8E_6A1DL,
        /*  267 */ 0x4209_6CCC_8F6A_C048L, 0x7A16_1E42_BFA5_21B1L,
        /*  268 */ 0x69A8_AE14_18AA_CD41L, 0x4356_96D1_32A1_CF81L,
        /*  269 */ 0x5486_F1A9_AD55_7101L, 0x1C45_4574_2881_72CEL,
        /*  270 */ 0x439F_27BA_F111_2734L, 0x169D_D129_BA01_28A5L,
        /*  271 */ 0x6C31_D92B_1B4E_A520L, 0x242F_B50F_9001_DAA1L,
        /*  272 */ 0x568E_4755_AF72_1DB3L, 0x368C_90D9_4001_7BB4L,
        /*  273 */ 0x453E_9F77_BF8E_7E29L, 0x120A_0D7A_999A_C95DL,
        /*  274 */ 0x6ECA_98BF_98E3_FD0EL, 0x5010_1590_F5C4_7561L,
        /*  275 */ 0x58A2_13CC_7A4F_FDA5L, 0x2673_4473_F7D0_5DE8L,
        /*  276 */ 0x46E8_0FD6_C83F_FE1DL, 0x6B8F_69F6_5FD9_E4B9L,
        /*  277 */ 0x7173_4C8A_D9FF_FCFCL, 0x45B2_4323_CC8F_D45CL,
        /*  278 */ 0x5AC2_A3A2_47FF_FD96L, 0x6AF5_0283_0A0C_A9E3L,
        /*  279 */ 0x489B_B61B_6CCC_CADFL, 0x08C4_0202_6E70_87E9L,
        /*  280 */ 0x742C_5692_47AE_1164L, 0x746C_D003_E3E7_3FDBL,
        /*  281 */ 0x5CF0_4541_D2F1_A783L, 0x76BD_7336_4FEC_3315L,
        /*  282 */ 0x4A59_D101_758E_1F9CL, 0x5EFD_F5C5_0CBC_F5ABL,
        /*  283 */ 0x76F6_1B35_88E3_65C7L, 0x4B2F_EFA1_ADFB_22ABL,
        /*  284 */ 0x5F2B_48F7_A0B5_EB06L, 0x08F3_261A_F195_B555L,
        /*  285 */ 0x4C22_A0C6_1A2B_226BL, 0x20C2_84E2_5ADE_2AABL,
        /*  286 */ 0x79D1_013C_F6AB_6A45L, 0x1AD0_D49D_5E30_4444L,
        /*  287 */ 0x6174_00FD_9222_BB6AL, 0x48A7_107D_E4F3_69D0L,
        /*  288 */ 0x4DF6_6731_41B5_62BBL, 0x53B8_D9FE_50C2_BB0DL,
        /*  289 */ 0x7CBD_71E8_6922_3792L, 0x52C1_5CCA_1AD1_2B48L,
        /*  290 */ 0x63CA_C186_BA81_C60EL, 0x7567_7D6E_7BDA_8906L,
        /*  291 */ 0x4FD5_679E_FB9B_04D8L, 0x5DEC_6458_6315_3A6CL,
        /*  292 */ 0x7FBB_D8FE_5F5E_6E27L, 0x497A_3A27_04EE_C3DFL,
    };

}
