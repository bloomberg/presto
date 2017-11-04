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
package com.facebook.presto.accumulo;

import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.spi.eventlistener.EventListenerFactory;
import io.airlift.units.Duration;

import java.util.Map;

import static com.facebook.presto.accumulo.conf.AccumuloConfig.INSTANCE;
import static com.facebook.presto.accumulo.conf.AccumuloConfig.PASSWORD;
import static com.facebook.presto.accumulo.conf.AccumuloConfig.USERNAME;
import static com.facebook.presto.accumulo.conf.AccumuloConfig.ZOOKEEPERS;

public class AccumuloEventListenerFactory
        implements EventListenerFactory
{
    private static final String NAME = "accumulo-event-sink";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public EventListener create(Map<String, String> config)
    {
        // We set the test details here as defaults since the map of configs is always empty
        return new AccumuloEventListener(
                config.getOrDefault(INSTANCE, "miniInstance"),
                config.getOrDefault(ZOOKEEPERS, "localhost:21810"),
                config.getOrDefault(USERNAME, "root"),
                config.getOrDefault(PASSWORD, "secret"),
                Duration.valueOf(config.getOrDefault("accumulo.timeout", "1m")),
                Duration.valueOf(config.getOrDefault("accumulo.latency", "10s")),
                config.getOrDefault("accumulo.query.archive.table.name", "query_archive"));
    }
}
