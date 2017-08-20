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

import java.util.Arrays;

import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.fail;

public class TestPostfixedIndexStorage
{
    @Test()
    public void testSameValueDifferentEncodings()
    {
        PostfixedIndexStorage storage = new PostfixedIndexStorage(100);

        for (int i = 0; i < 100; ++i) {
            if (!Arrays.equals(storage.encode("test".getBytes(UTF_8)), storage.encode("test".getBytes(UTF_8)))) {
                return;
            }
        }

        fail("Encoding of same value did not result in a different string at all in 100 tests");
    }

    @Test
    public void testPostfixOneByte()
    {
        PostfixedIndexStorage storage = new PostfixedIndexStorage(1);
        byte[] encoded1 = storage.encode("test".getBytes(UTF_8));
        byte[] encoded2 = storage.encode("othertest".getBytes(UTF_8));
        byte[] encoded3 = storage.encode("whynotzoidberg".getBytes(UTF_8));

        assertEquals(encoded1.length, 5);
        assertEquals(encoded2.length, 10);
        assertEquals(encoded3.length, 15);

        assertEquals(new String(storage.decode(encoded1), UTF_8), "test");
        assertEquals(new String(storage.decode(encoded2), UTF_8), "othertest");
        assertEquals(new String(storage.decode(encoded3), UTF_8), "whynotzoidberg");
    }

    @Test
    public void testPostfixFourBytes()
    {
        PostfixedIndexStorage storage = new PostfixedIndexStorage(4);
        byte[] encoded1 = storage.encode("test".getBytes(UTF_8));
        byte[] encoded2 = storage.encode("othertest".getBytes(UTF_8));
        byte[] encoded3 = storage.encode("whynotzoidberg".getBytes(UTF_8));

        assertEquals(encoded1.length, 8);
        assertEquals(encoded2.length, 13);
        assertEquals(encoded3.length, 18);

        assertEquals(new String(storage.decode(encoded1), UTF_8), "test");
        assertEquals(new String(storage.decode(encoded2), UTF_8), "othertest");
        assertEquals(new String(storage.decode(encoded3), UTF_8), "whynotzoidberg");
    }

    @Test
    public void testPostfixEightBytes()
    {
        PostfixedIndexStorage storage = new PostfixedIndexStorage(8);
        byte[] encoded1 = storage.encode("test".getBytes(UTF_8));
        byte[] encoded2 = storage.encode("othertest".getBytes(UTF_8));
        byte[] encoded3 = storage.encode("whynotzoidberg".getBytes(UTF_8));

        assertEquals(encoded1.length, 12);
        assertEquals(encoded2.length, 17);
        assertEquals(encoded3.length, 22);

        assertEquals(new String(storage.decode(encoded1), UTF_8), "test");
        assertEquals(new String(storage.decode(encoded2), UTF_8), "othertest");
        assertEquals(new String(storage.decode(encoded3), UTF_8), "whynotzoidberg");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativeBytes()
    {
        new PostfixedIndexStorage(-1);
    }
}
