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
package com.facebook.presto.plugin.oracle;

import io.airlift.configuration.Config;

public class OracleConfig
{
    private boolean includeSynonyms = true;

    private int defaultRowPrefetch = 5000;

    public boolean isIncludeSynonyms()
    {
        return includeSynonyms;
    }

    @Config("oracle.include-synonyms")
    public OracleConfig setIncludeSynonyms(boolean includeSynonyms)
    {
        this.includeSynonyms = includeSynonyms;
        return this;
    }

    public int getDefaultRowPrefetch()
    {
        return defaultRowPrefetch;
    }

    @Config("oracle.fetch-size")
    public OracleConfig setDefaultRowPrefetch(int fetchSize)
    {
        this.defaultRowPrefetch = fetchSize;
        return this;
    }
}
