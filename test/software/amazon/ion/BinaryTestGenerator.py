#!/apollo/bin/env -e Python python

import struct

_COOKIE = struct.pack('>L', 0x10140100L)

def var_uint(val) :
    assert isinstance(val, (int, long))
    assert val >= 0
    bytes = []
    while val > 0x7F :
        chunk = (val & 0x7F)
        bytes.append(chunk)
        val >>= 7
    bytes.append(val)
    # terminating high bit
    bytes[0] |= 0x80
    return ''.join(chr(x) for x in reversed(bytes))

def var_int(val) :
    assert isinstance(val, (int, long))
    bytes = []
    if val < 0 :
        val = -val
        sbit = 0x40
    else :
        sbit = 0
    while val > 0x7F :
        chunk = (val & 0x7F)
        bytes.append(chunk)
        val >>= 7
    if val > 0x3F :
        # can't quite fit the sign bit
        bytes.append(val)
        bytes.append(sbit)
    else :
        bytes.append(val | sbit)
    # terminating high bit
    bytes[0] |= 0x80
    return ''.join(chr(x) for x in reversed(bytes))

def raw_uint(val) :
    assert isinstance(val, (int, long))
    assert val >= 0
    bytes = []
    while val > 0xFF :
        bytes.append(val & 0xFF)
        val >>= 8
    bytes.append(val)
    return ''.join(chr(x) for x in reversed(bytes))

def raw_int(val) :
    assert isinstance(val, (int, long))
    bytes = []
    if val < 1 :
        val = -val
        sbit = 0x40
    else :
        sbit = 0
    while val > 0xFF :
        bytes.append(chr(val & 0xFF))
        val >>= 8
    if val > 0x7F :
        # can't quite fit the sign bit
        bytes.append(val)
        bytes.append(sbit)
    else :
        bytes.append(val | sbit)
    return ''.join(chr(x) for x in reversed(bytes))

def gen_type(high, low) :
    assert high >= 0 and high <= 15
    assert low >= 0 and low <= 15
    return struct.pack('>B', ((high << 4 ) | low))

_LN_EXT     = 14
_LN_NULL    = 15

def gen_type_len(high, len) :
    assert high >= 0 and high <= 15
    assert len is None or len >= 0
    if len is None :
        return gen_type(high, _LN_NULL)
    elif len < _LN_EXT :
        return gen_type(high, len)
    else :
        return gen_type(high, _LN_EXT) + var_uint(len)

def gen_null(high) :
    return gen_type(high, _LN_NULL)

_HN_NULL        = 0
_HN_BOOL        = 1
_HN_PINT        = 2
_HN_NINT        = 3
_HN_FLOAT       = 4
_HN_DECIMAL     = 5
_HN_TIMESTAMP   = 6
_HN_SYMBOL      = 7
_HN_STRING      = 8
_HN_CLOB        = 9
_HN_BLOB        = 10
_HN_LIST        = 11
_HN_SEXP        = 12
_HN_STRUCT      = 13
_HN_ANNOTATION  = 14

def gen_int(val) :
    assert val is None or isinstance(val, (int, long))
    if val is None :
        return gen_null(_HN_PINT)
    
    if val < 0 :
        val = -val
        type = _HN_NINT
    else :
        type = _HN_PINT
    
    if val != 0 :
        raw_bits = raw_uint(val)
    else :
        raw_bits = ''
    return gen_type_len(type, len(raw_bits)) \
            + raw_bits

def gen_float(val) :
    assert val is None or isinstance(val, (float, int))
    if val is None :
        return gen_null(_HN_FLOAT)

    if isinstance(val, int) :
        val = float(int)
    # note this violates the standard
    return gen_type(_HN_FLOAT, 8) + struct.pack('>d', val)

def gen_raw_decimal(val) :
    from decimal import Decimal
    assert isinstance(val, (str, int, long, Decimal))
    if isinstance(val, (str, int, long)) :
        val = Decimal(val)
    sign, digits, exponent = val.as_tuple()
    mantissa = 0L
    for digit in digits :
        mantissa *= 10
        mantissa += digit
    if sign == 1 :
        mantissa = -mantissa
    print mantissa, exponent
    raw_mantissa = raw_int(mantissa)
    raw_exponent = var_int(exponent)
    return raw_exponent + raw_mantissa

def gen_decimal(val) :
    from decimal import Decimal
    assert val is None or isinstance(val, (str, int, long, Decimal))
    if val is None :
        return gen_null(_HN_DECIMAL)

    raw_decimal = gen_raw_decimal(val)
    return gen_type_len(_HN_DECIMAL, len(raw_decimal)) \
            + raw_decimal

def gen_timestamp(milliseconds, offset = 0) :
    from decimal import Decimal
    assert milliseconds is None or isinstance(milliseconds, (str, int, long, Decimal))
    assert isinstance(offset, (int, long))
    if milliseconds is None :
        return gen_null(_HN_TIMESTAMP)

    raw_offset = var_int(offset)
    raw_ms = gen_raw_decimal(milliseconds)
    return gen_type_len(_HN_TIMESTAMP, len(raw_offset) + len(raw_ms)) \
            + raw_offset \
            + raw_ms

def gen_symbol(val) :
    assert val is None or isinstance(val, (int, long))
    # symbols must be greater than 0
    assert val > 0
    if val is None :
        return gen_null(_HN_SYMBOL)

    raw_symbol = raw_uint(val)
    return gen_type_len(_HN_SYMBOL, len(raw_symbol)) \
            + raw_symbol

def gen_byte_type(type, val) :
    assert val is None or isinstance(val, str)
    if val is None :
        return gen_null(type)

    return gen_type_len(type, len(val)) + val

def gen_string(val) :
    assert val is None or isinstance(val, (str, unicode))
    if isinstance(val, unicode):
        val = val.encode('UTF-8')
    else :
        try :
            val.decode('UTF-8')
        except UnicodeDecodeError :
            raise ValueError, 'Byte string must be UTF-8'
    return gen_byte_type(_HN_STRING, val)

def gen_clob(val) :
    assert val is None or isinstance(val, str)
    return gen_byte_type(_HN_CLOB, val)

def gen_blob(val) :
    assert val is None or isinstance(val, str)
    return gen_byte_type(_HN_BLOB, val)

def gen_sequence_type(type, *blobs) :
    assert all(isinstance(blob, str) for blob in blobs)
    return gen_type_len(type, sum(len(blob) for blob in blobs)) \
            + ''.join(blobs)

def gen_list(*blobs) :
    return gen_sequence_type(_HN_LIST, *blobs)

def gen_sexp(*blobs) :
    return gen_sequence_type(_HN_SEXP, *blobs)

def gen_struct(*pairs) :
    assert all(isinstance(id, (int, long)) \
            and id > 0 and isinstance(bytes, str) \
                for id, bytes in pairs)
    data = ''.join('%s%s' % (var_uint(id), bytes) for id, bytes in pairs)
    data_len = len(data)
    return gen_type_len(_HN_STRUCT, data_len) + data

def gen_ann(bytes, *syms) :
    assert isinstance(bytes, str)
    assert all(isinstance(x, (int, long)) and x > 0 for x in syms)
    anns = ''.join(var_uint(x) for x in syms)
    anns_len = len(anns)
    anns_len_bytes = var_uint(anns_len)
    val_len = len(anns_len_bytes) + anns_len + len(bytes)
    return gen_type_len(_HN_ANNOTATION, val_len) \
            + anns_len_bytes \
            + anns \
            + bytes

def lit(*vals):
    from itertools import chain
    return '"%s"' % ' '.join('%02X' % ord(x) for x in chain(*vals))

_SYM_ION                = 1
_SYM_ION_1_0            = 2
_SYM_ION_SYMBOL_TABLE   = 3
_SYM_NAME               = 4
_SYM_VERSION            = 5
_SYM_IMPORTS            = 6
_SYM_SYMBOLS            = 7
_SYM_MAX_ID             = 8

_SYS_SYMBOLS            = 9;

if __name__ == '__main__' :
    print '$ion_1_0::{symbols : struct.null} 1e0'
    print lit(
        gen_ann(
            gen_struct(
                (_SYM_SYMBOLS, gen_null(_HN_STRUCT))
            ),
            _SYM_ION_1_0),
        gen_float(1.0)
    )
    print lit(
        gen_decimal(1)
    )
