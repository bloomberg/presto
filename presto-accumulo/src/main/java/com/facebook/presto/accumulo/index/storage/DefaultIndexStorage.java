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

/**
 * This storage strategy performs no mutations to the given data.
 */
public class DefaultIndexStorage
        implements IndexStorage
{
    public byte[] encode(byte[] bytes)
    {
        return bytes;
    }

    public byte[] decode(byte[] bytes)
    {
        return bytes;
    }
}
