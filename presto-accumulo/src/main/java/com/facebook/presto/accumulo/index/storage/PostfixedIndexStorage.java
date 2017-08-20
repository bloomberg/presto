/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.accumulo.index.storage;

import com.google.common.primitives.Bytes;

import java.util.Arrays;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This storage strategy performs no mutations to the given data.
 */
public class PostfixedIndexStorage
        implements IndexStorage
{
    private int numBytes;
    private byte[] postfixBytes;
    private Random random = new Random();

    public PostfixedIndexStorage(int numBytes)
    {
        checkArgument(numBytes > 0, "Number of chars must be greater than zero");
        this.numBytes = numBytes;
        this.postfixBytes = new byte[numBytes];
    }

    public byte[] encode(byte[] bytes)
    {
        random.nextBytes(postfixBytes);
        return Bytes.concat(bytes, postfixBytes);
    }

    public byte[] decode(byte[] bytes)
    {
        return Arrays.copyOfRange(bytes, 0, bytes.length - numBytes);
    }
}
