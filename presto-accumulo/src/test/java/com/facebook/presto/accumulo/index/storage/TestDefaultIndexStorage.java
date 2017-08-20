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

public class TestDefaultIndexStorage
{
    @Test
    public void testEncodedIsSame()
    {
        DefaultIndexStorage storage = new DefaultIndexStorage();
        byte[] encoded1 = storage.encode("test".getBytes(UTF_8));
        byte[] encoded2 = storage.encode("othertest".getBytes(UTF_8));
        byte[] encoded3 = storage.encode("whynotzoidberg".getBytes(UTF_8));

        assertEquals(new String(encoded1, UTF_8), "test");
        assertEquals(new String(encoded2, UTF_8), "othertest");
        assertEquals(new String(encoded3, UTF_8), "whynotzoidberg");

        assertEquals(new String(storage.decode(encoded1), UTF_8), "test");
        assertEquals(new String(storage.decode(encoded2), UTF_8), "othertest");
        assertEquals(new String(storage.decode(encoded3), UTF_8), "whynotzoidberg");
    }
}
