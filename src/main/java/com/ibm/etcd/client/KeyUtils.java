/*
 * Copyright 2017, 2018 IBM Corp. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.ibm.etcd.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

/**
 * Helper methods for manipulating ByteString keys
 */
public final class KeyUtils {

    private KeyUtils() {} // static only

    public static final ByteString ZERO_BYTE = singleByte(0);

    public static ByteString plusOne(ByteString key) {
        int max = key.size() - 1;
        if (max < 0) {
            return singleByte(1);
        }
        int lastPlusOne = key.byteAt(max) + 1;
        ByteString excludeLast = key.substring(0, max);
        return lastPlusOne == 0 ? plusOne(excludeLast)
                : excludeLast.concat(singleByte(lastPlusOne));
    }

    public static ByteString singleByte(int b) {
        return UnsafeByteOperations.unsafeWrap(new byte[] { (byte) b });
    }

    public static int compareByteStrings(ByteString bs1, ByteString bs2) {
        int s1 = bs1.size(), s2 = bs2.size(), n = Math.min(s1, s2);
        for (int i = 0; i < n; i++) {
            int cmp = (bs1.byteAt(i) & 0xff) - (bs2.byteAt(i) & 0xff);
            if (cmp != 0) {
                return cmp;
            }
        }
        return s1 - s2;
    }

    public static ByteString bs(String str) {
        return str != null ? ByteString.copyFromUtf8(str) : null;
    }

    private static final String HEX_CHARS_STR = "0123456789abcdef";
    private static final char[] HEX_CHARS = HEX_CHARS_STR.toCharArray();

    public static String toHexString(ByteString bs) {
        int len = bs.size();
        if (len == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(len << 1);
        for (int i = 0; i < len; i++) {
            int b = bs.byteAt(i);
            sb.append(HEX_CHARS[(b >> 4) & 0xf]).append(HEX_CHARS[b & 0xf]);
        }
        return sb.toString();
    }

    public static ByteString fromHexString(CharSequence seq) {
        int len = seq.length();
        if (len == 0) {
            return ByteString.EMPTY;
        }
        if (len % 2 != 0) {
            throw new IllegalArgumentException("must be even number of chars");
        }
        int blen = len >> 1;
        byte[] bytes = new byte[blen];
        for (int i = 0, j = 0; i < blen; i ++, j += 2) {
            bytes[i] = (byte) ((digitFor(seq.charAt(j)) << 4) | digitFor(seq.charAt(j + 1)));
        }
        return UnsafeByteOperations.unsafeWrap(bytes);
    }

    private static int digitFor(char c) {
        int d = HEX_CHARS_STR.indexOf(Character.toLowerCase(c));
        if (d == -1) {
            throw new IllegalArgumentException("invalid char: " + c);
        }
        return d;
    }

    //TODO other stuff for namespaces etc
}
