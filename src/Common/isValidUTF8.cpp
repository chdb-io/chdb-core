#include <Common/isValidUTF8.h>
#include <cstring>

#ifdef __SSE4_1__
#include <emmintrin.h>
#include <smmintrin.h>
#include <tmmintrin.h>
#endif

/// inspired by https://github.com/cyb70289/utf8/

/*
MIT License

Copyright (c) 2019 Yibo Cai

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

/*
* http://www.unicode.org/versions/Unicode6.0.0/ch03.pdf - page 94
*
* Table 3-7. Well-Formed UTF-8 Byte Sequences
*
* +--------------------+------------+-------------+------------+-------------+
* | Code Points        | First Byte | Second Byte | Third Byte | Fourth Byte |
* +--------------------+------------+-------------+------------+-------------+
* | U+0000..U+007F     | 00..7F     |             |            |             |
* +--------------------+------------+-------------+------------+-------------+
* | U+0080..U+07FF     | C2..DF     | 80..BF      |            |             |
* +--------------------+------------+-------------+------------+-------------+
* | U+0800..U+0FFF     | E0         | A0..BF      | 80..BF     |             |
* +--------------------+------------+-------------+------------+-------------+
* | U+1000..U+CFFF     | E1..EC     | 80..BF      | 80..BF     |             |
* +--------------------+------------+-------------+------------+-------------+
* | U+D000..U+D7FF     | ED         | 80..9F      | 80..BF     |             |
* +--------------------+------------+-------------+------------+-------------+
* | U+E000..U+FFFF     | EE..EF     | 80..BF      | 80..BF     |             |
* +--------------------+------------+-------------+------------+-------------+
* | U+10000..U+3FFFF   | F0         | 90..BF      | 80..BF     | 80..BF      |
* +--------------------+------------+-------------+------------+-------------+
* | U+40000..U+FFFFF   | F1..F3     | 80..BF      | 80..BF     | 80..BF      |
* +--------------------+------------+-------------+------------+-------------+
* | U+100000..U+10FFFF | F4         | 80..8F      | 80..BF     | 80..BF      |
* +--------------------+------------+-------------+------------+-------------+
*/
namespace DB
{

namespace UTF8
{

#ifndef __SSE4_1__
namespace
{

UInt8 isValidUTF8Scalar(const UInt8 * data, UInt64 len)
{
    while (len)
    {
        int bytes;
        const UInt8 byte1 = data[0];
        /* 00..7F */
        if (byte1 <= 0x7F)
        {
            bytes = 1;
        }
        /* C2..DF, 80..BF */
        else if (len >= 2 && byte1 >= 0xC2 && byte1 <= 0xDF && static_cast<Int8>(data[1]) <= static_cast<Int8>(0xBF))
        {
            bytes = 2;
        }
        else if (len >= 3)
        {
            const UInt8 byte2 = data[1];
            bool byte2_ok = static_cast<Int8>(byte2) <= static_cast<Int8>(0xBF);
            bool byte3_ok = static_cast<Int8>(data[2]) <= static_cast<Int8>(0xBF);

            if (byte2_ok && byte3_ok &&
                /* E0, A0..BF, 80..BF */
                ((byte1 == 0xE0 && byte2 >= 0xA0) ||
                 /* E1..EC, 80..BF, 80..BF */
                 (byte1 >= 0xE1 && byte1 <= 0xEC) ||
                 /* ED, 80..9F, 80..BF */
                 (byte1 == 0xED && byte2 <= 0x9F) ||
                 /* EE..EF, 80..BF, 80..BF */
                 (byte1 >= 0xEE && byte1 <= 0xEF)))
            {
                bytes = 3;
            }
            else if (len >= 4)
            {
                bool byte4_ok = static_cast<Int8>(data[3]) <= static_cast<Int8>(0xBF);
                if (byte2_ok && byte3_ok && byte4_ok &&
                    /* F0, 90..BF, 80..BF, 80..BF */
                    ((byte1 == 0xF0 && byte2 >= 0x90) ||
                     /* F1..F3, 80..BF, 80..BF, 80..BF */
                     (byte1 >= 0xF1 && byte1 <= 0xF3) ||
                     /* F4, 80..8F, 80..BF, 80..BF */
                     (byte1 == 0xF4 && byte2 <= 0x8F)))
                {
                    bytes = 4;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }
        else
        {
            return false;
        }
        len -= bytes;
        data += bytes;
    }
    return true;
}

}
#endif

#ifdef __SSE4_1__

/// SSE4.1 range-table validator (Yibo Cai algorithm). ~12-18 GB/s vs the
/// scalar version's 2-5 GB/s. Extracted from src/Functions/isValidUTF8.cpp
/// so callers in the emit / Arrow output path get the fast path too.
UInt8 isValidUTF8(const UInt8 * data, UInt64 len)
{
    const __m128i first_len_tbl = _mm_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3);
    const __m128i first_range_tbl = _mm_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 8, 8, 8, 8);
    const __m128i range_min_tbl
        = _mm_setr_epi8(0x00, 0x80, 0x80, 0x80, 0xA0, 0x80, 0x90, 0x80, 0xC2, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F, 0x7F);
    const __m128i range_max_tbl
        = _mm_setr_epi8(0x7F, 0xBF, 0xBF, 0xBF, 0xBF, 0x9F, 0xBF, 0x8F, 0xF4, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80);
    const __m128i df_ee_tbl = _mm_setr_epi8(0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0);
    const __m128i ef_fe_tbl = _mm_setr_epi8(0, 3, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    __m128i prev_input = _mm_set1_epi8(0);
    __m128i prev_first_len = _mm_set1_epi8(0);
    __m128i error = _mm_set1_epi8(0);

    auto check_packed = [&](__m128i input) noexcept
    {
        const __m128i high_nibbles = _mm_and_si128(_mm_srli_epi16(input, 4), _mm_set1_epi8(0x0F));
        __m128i first_len = _mm_shuffle_epi8(first_len_tbl, high_nibbles);
        __m128i range = _mm_shuffle_epi8(first_range_tbl, high_nibbles);
        range = _mm_or_si128(range, _mm_alignr_epi8(first_len, prev_first_len, 15));

        __m128i tmp1 = _mm_subs_epu8(first_len, _mm_set1_epi8(1));
        __m128i tmp2 = _mm_subs_epu8(prev_first_len, _mm_set1_epi8(1));
        range = _mm_or_si128(range, _mm_alignr_epi8(tmp1, tmp2, 14));

        tmp1 = _mm_subs_epu8(first_len, _mm_set1_epi8(2));
        tmp2 = _mm_subs_epu8(prev_first_len, _mm_set1_epi8(2));
        range = _mm_or_si128(range, _mm_alignr_epi8(tmp1, tmp2, 13));

        __m128i shift1 = _mm_alignr_epi8(input, prev_input, 15);
        __m128i pos = _mm_sub_epi8(shift1, _mm_set1_epi8(0xEF));
        tmp1 = _mm_subs_epu8(pos, _mm_set1_epi8(0xF0));
        __m128i range2 = _mm_shuffle_epi8(df_ee_tbl, tmp1);
        tmp2 = _mm_adds_epu8(pos, _mm_set1_epi8(112));
        range2 = _mm_add_epi8(range2, _mm_shuffle_epi8(ef_fe_tbl, tmp2));
        range = _mm_add_epi8(range, range2);

        __m128i minv = _mm_shuffle_epi8(range_min_tbl, range);
        __m128i maxv = _mm_shuffle_epi8(range_max_tbl, range);

        error = _mm_or_si128(error, _mm_cmplt_epi8(input, minv));
        error = _mm_or_si128(error, _mm_cmpgt_epi8(input, maxv));

        prev_input = input;
        prev_first_len = first_len;
        data += 16;
        len -= 16;
    };

    while (len >= 16) // NOLINT
        check_packed(_mm_loadu_si128(reinterpret_cast<const __m128i *>(data)));

    /// 0 <= len <= 15 tail: load from (data-1) using buffer padding semantics,
    /// zero the unknown bytes past the end, then re-run check_packed.
    alignas(16) char buf[32];
    _mm_store_si128(reinterpret_cast<__m128i *>(buf), _mm_loadu_si128(reinterpret_cast<const __m128i *>(data - 1)));
    memset(buf + len + 1, 0, 16);
    check_packed(_mm_loadu_si128(reinterpret_cast<__m128i *>(buf + 1)));

    return static_cast<UInt8>(_mm_testz_si128(error, error));
}

#else

UInt8 isValidUTF8(const UInt8 * data, UInt64 len)
{
    return isValidUTF8Scalar(data, len);
}

#endif

}
}
