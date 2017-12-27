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
package com.facebook.presto.accumulo.io;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class NullPageSource
        implements UpdatablePageSource
{
    @Override
    public void deleteRows(Block rowIds)
    { }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(ImmutableList.of());
    }

    @Override
    public long getTotalBytes()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return true;
    }

    @Override
    public Page getNextPage()
    {
        return null;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        getFutureValue(finish());
    }
}