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

import org.testng.annotations.Test;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertTrue;

public class TestShardedIndexStorage
{
    @Test
    public void testBinaryStorage()
    {
        ShardedIndexStorage storage = new ShardedIndexStorage(2);
        for (int i = 0; i < 100; ++i) {
            char shard = new String(storage.encode("test".getBytes(UTF_8)), UTF_8).charAt(0);
            assertTrue(shard == '0' || shard == '1');
        }
    }

    @Test
    public void testSameValueSameShard()
    {
        ShardedIndexStorage storage = new ShardedIndexStorage(2);
        assertEquals(new String(storage.encode("test".getBytes(UTF_8)), UTF_8), "1test");
        assertEquals(new String(storage.encode("test".getBytes(UTF_8)), UTF_8), "1test");
        assertEquals(new String(storage.encode("test".getBytes(UTF_8)), UTF_8), "1test");

        assertEquals(new String(storage.decode("1test".getBytes(UTF_8)), UTF_8), "test");
        assertEquals(new String(storage.decode("1test".getBytes(UTF_8)), UTF_8), "test");
        assertEquals(new String(storage.decode("1test".getBytes(UTF_8)), UTF_8), "test");
    }

    @Test
    public void testSingleDigitShards()
    {
        ShardedIndexStorage storage = new ShardedIndexStorage(10);
        assertEquals(new String(storage.encode("test".getBytes(UTF_8)), UTF_8).length(), 5);
        assertEquals(new String(storage.encode("othertest".getBytes(UTF_8)), UTF_8).length(), 10);
        assertEquals(new String(storage.encode("whynotzoidberg".getBytes(UTF_8)), UTF_8).length(), 15);

        assertEquals(new String(storage.encode("test".getBytes(UTF_8)), UTF_8), "9test");
        assertEquals(new String(storage.encode("othertest".getBytes(UTF_8)), UTF_8), "9othertest");
        assertEquals(new String(storage.encode("whynotzoidberg".getBytes(UTF_8)), UTF_8), "8whynotzoidberg");

        assertEquals(new String(storage.decode("9test".getBytes(UTF_8)), UTF_8), "test");
        assertEquals(new String(storage.decode("9othertest".getBytes(UTF_8)), UTF_8), "othertest");
        assertEquals(new String(storage.decode("8whynotzoidberg".getBytes(UTF_8)), UTF_8), "whynotzoidberg");
    }

    @Test
    public void testDoubleDigitShards()
    {
        ShardedIndexStorage storage = new ShardedIndexStorage(100);
        assertEquals(new String(storage.encode("test".getBytes(UTF_8)), UTF_8).length(), 6);
        assertEquals(new String(storage.encode("othertest".getBytes(UTF_8)), UTF_8).length(), 11);
        assertEquals(new String(storage.encode("whynotzoidberg".getBytes(UTF_8)), UTF_8).length(), 16);

        assertEquals(new String(storage.encode("test".getBytes(UTF_8)), UTF_8), "19test");
        assertEquals(new String(storage.encode("othertest".getBytes(UTF_8)), UTF_8), "79othertest");
        assertEquals(new String(storage.encode("whynotzoidberg".getBytes(UTF_8)), UTF_8), "08whynotzoidberg");

        assertEquals(new String(storage.decode("19test".getBytes(UTF_8)), UTF_8), "test");
        assertEquals(new String(storage.decode("79othertest".getBytes(UTF_8)), UTF_8), "othertest");
        assertEquals(new String(storage.decode("08whynotzoidberg".getBytes(UTF_8)), UTF_8), "whynotzoidberg");
    }

    @Test
    public void testTripleDigitShards()
    {
        ShardedIndexStorage storage = new ShardedIndexStorage(1000);
        assertEquals(new String(storage.encode("test".getBytes(UTF_8)), UTF_8).length(), 7);
        assertEquals(new String(storage.encode("othertest".getBytes(UTF_8)), UTF_8).length(), 12);
        assertEquals(new String(storage.encode("whynotzoidberg".getBytes(UTF_8)), UTF_8).length(), 17);

        assertEquals(new String(storage.encode("test".getBytes(UTF_8)), UTF_8), "019test");
        assertEquals(new String(storage.encode("othertest".getBytes(UTF_8)), UTF_8), "879othertest");
        assertEquals(new String(storage.encode("whynotzoidberg".getBytes(UTF_8)), UTF_8), "108whynotzoidberg");

        assertEquals(new String(storage.decode("019test".getBytes(UTF_8)), UTF_8), "test");
        assertEquals(new String(storage.decode("879othertest".getBytes(UTF_8)), UTF_8), "othertest");
        assertEquals(new String(storage.decode("108whynotzoidberg".getBytes(UTF_8)), UTF_8), "whynotzoidberg");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testZeroShards()
    {
        new ShardedIndexStorage(0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativeShards()
    {
        new ShardedIndexStorage(-1);
    }
}
