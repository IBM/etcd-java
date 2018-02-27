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

/**
 * Helper methods for manipulating ByteString keys
 */
import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;

public class KeyUtils {

    private KeyUtils() {} // static only
    
    public static final ByteString ZERO_BYTE = singleByte(0);
    
    public static ByteString plusOne(ByteString key) {
        int max = key.size()-1;
        if(max < 0) return singleByte(1);
        int last = key.byteAt(max);
        ByteString excludeLast = key.substring(0, max);
        return last == 0xff ? plusOne(excludeLast)
                : excludeLast.concat(singleByte(last+1));
    }
    
    public static ByteString singleByte(int b) {
        return UnsafeByteOperations.unsafeWrap(new byte[]{(byte)b});
    }
    
    public static int compareByteStrings(ByteString bs1, ByteString bs2) {
        int s1 = bs1.size(), s2 = bs2.size(), n = Math.min(s1, s2);
        for (int i = 0 ; i < n ; i++) {
            int cmp = Byte.compare(bs1.byteAt(i), bs2.byteAt(i));
            if (cmp != 0) return cmp;
        }
        return s1 - s2;
    }
    
    //TODO other stuff for namespaces etc
}
