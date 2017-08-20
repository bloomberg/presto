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

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.abs;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This storage strategy performs no mutations to the given data.
 */
public class ShardedIndexStorage
        implements IndexStorage
{
    private int numShards;
    private int cropLength;
    private String formatString;

    public ShardedIndexStorage(int numShards)
    {
        checkArgument(numShards > 1, "Number of shards must be greater than one");
        this.numShards = numShards;
        this.cropLength = Integer.toString(numShards - 1).length();
        this.formatString = "%0" + cropLength + "d";
    }

    public byte[] encode(byte[] bytes)
    {
        return Bytes.concat(String.format(formatString, abs(Arrays.hashCode(bytes)) % numShards).getBytes(UTF_8), bytes);
    }

    public byte[] decode(byte[] bytes)
    {
        return Arrays.copyOfRange(bytes, cropLength, bytes.length);
    }
}
