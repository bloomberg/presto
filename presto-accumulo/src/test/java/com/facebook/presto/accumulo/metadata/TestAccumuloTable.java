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
package com.facebook.presto.accumulo.metadata;

import com.facebook.presto.accumulo.index.metrics.AccumuloMetricsStorage;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.serializers.LexicoderRowSerializer;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;

public class TestAccumuloTable
{
    private final JsonCodec<AccumuloTable> codec;

    AccumuloColumnHandle c1 = new AccumuloColumnHandle("id", Optional.empty(), Optional.empty(), VARCHAR, 0, "");
    AccumuloColumnHandle c2 = new AccumuloColumnHandle("age", Optional.of("cf"), Optional.of("age"), BIGINT, 1, "");
    AccumuloColumnHandle c3 = new AccumuloColumnHandle("firstname", Optional.of("cf"), Optional.of("firstname"), VARCHAR, 2, "");
    AccumuloColumnHandle c4 = new AccumuloColumnHandle("arr", Optional.of("cf"), Optional.of("arr"), new ArrayType(VARCHAR), 3, "");
    AccumuloColumnHandle c5 = new AccumuloColumnHandle("born", Optional.of("cf"), Optional.of("born"), TIMESTAMP, 4, "");

    public TestAccumuloTable()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setJsonDeserializers(ImmutableMap.of(Type.class, new TestingTypeDeserializer()));
        JsonCodecFactory codecFactory = new JsonCodecFactory(provider);
        codec = codecFactory.jsonCodec(AccumuloTable.class);
    }

    @Test
    public void testJsonRoundTrip()
    {
        AccumuloTable expected = new AccumuloTable(
                "default",
                "test_table",
                ImmutableList.of(c1, c2, c3, c4, c5),
                "id",
                false,
                LexicoderRowSerializer.class.getCanonicalName(),
                Optional.of("private,moreprivate"),
                Optional.of(AccumuloMetricsStorage.class.getCanonicalName()),
                true,
                Optional.of("age,firstname,arr,born"));

        String json = codec.toJson(expected);
        AccumuloTable actual = codec.fromJson(json);
        assertTable(actual, expected);
    }

    @Test
    public void testJsonRoundTripEmptyThings()
    {
        AccumuloTable expected = new AccumuloTable(
                "default",
                "test_table",
                ImmutableList.of(c1, c2, c3, c4, c5),
                "id",
                false,
                LexicoderRowSerializer.class.getCanonicalName(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty());

        String json = codec.toJson(expected);
        AccumuloTable actual = codec.fromJson(json);
        assertTable(actual, expected);
    }

    @Test
    public void testJsonCompositeIndexColumnDefined()
    {
        new AccumuloTable(
                "default",
                "test_table",
                ImmutableList.of(c1, c2, c3, c4, c5),
                "id",
                false,
                LexicoderRowSerializer.class.getCanonicalName(),
                Optional.of("private,moreprivate"),
                Optional.of(AccumuloMetricsStorage.class.getCanonicalName()),
                true,
                Optional.of("age,firstname,arr,born,firstname:age:born"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Row ID not_defined is not present in column definition")
    public void testJsonRowIdNotDefined()
    {
        new AccumuloTable(
                "default",
                "test_table",
                ImmutableList.of(c1, c2, c3, c4, c5),
                "not_defined",
                false,
                LexicoderRowSerializer.class.getCanonicalName(),
                Optional.of("private,moreprivate"),
                Optional.of(AccumuloMetricsStorage.class.getCanonicalName()),
                true,
                Optional.of("age,firstname,arr,born"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Specified index column is not defined: not_defined")
    public void testJsonIndexColumnNotDefined()
    {
        new AccumuloTable(
                "default",
                "test_table",
                ImmutableList.of(c1, c2, c3, c4, c5),
                "id",
                false,
                LexicoderRowSerializer.class.getCanonicalName(),
                Optional.of("private,moreprivate"),
                Optional.of(AccumuloMetricsStorage.class.getCanonicalName()),
                true,
                Optional.of("age,firstname,arr,born,not_defined"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Specified index column is not defined: not_defined")
    public void testJsonCompositeIndexColumnNotDefined()
    {
        new AccumuloTable(
                "default",
                "test_table",
                ImmutableList.of(c1, c2, c3, c4, c5),
                "id",
                false,
                LexicoderRowSerializer.class.getCanonicalName(),
                Optional.of("private,moreprivate"),
                Optional.of(AccumuloMetricsStorage.class.getCanonicalName()),
                true,
                Optional.of("age,firstname,arr,born,not_defined:born"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Timestamp-type columns must be at the end of a composite index")
    public void testJsonCompositeIndexTimestampNotLast()
    {
        new AccumuloTable(
                "default",
                "test_table",
                ImmutableList.of(c1, c2, c3, c4, c5),
                "id",
                false,
                LexicoderRowSerializer.class.getCanonicalName(),
                Optional.of("private,moreprivate"),
                Optional.of(AccumuloMetricsStorage.class.getCanonicalName()),
                true,
                Optional.of("age,firstname,arr,born,born:firstname"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Specified index column cannot be the row ID: id")
    public void testJsonCompositeIndexWithRowID()
    {
        new AccumuloTable(
                "default",
                "test_table",
                ImmutableList.of(c1, c2, c3, c4, c5),
                "id",
                false,
                LexicoderRowSerializer.class.getCanonicalName(),
                Optional.of("private,moreprivate"),
                Optional.of(AccumuloMetricsStorage.class.getCanonicalName()),
                true,
                Optional.of("age,firstname,arr,born,id:born"));
    }

    private static void assertTable(AccumuloTable actual, AccumuloTable expected)
    {
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getTable(), expected.getTable());
        assertEquals(actual.getColumns(), expected.getColumns());
        assertEquals(actual.getRowId(), expected.getRowId());
        assertEquals(actual.isExternal(), expected.isExternal());
        assertEquals(actual.getSerializerClassName(), expected.getSerializerClassName());
        assertEquals(actual.getScanAuthorizations(), expected.getScanAuthorizations());
        assertEquals(actual.getMetricsStorageClass(), expected.getMetricsStorageClass());
        assertEquals(actual.isTruncateTimestamps(), expected.isTruncateTimestamps());
        assertEquals(actual.getIndexColumns(), expected.getIndexColumns());
    }

    public static final class TestingTypeDeserializer
            extends FromStringDeserializer<Type>
    {
        private final Map<String, Type> types = ImmutableMap.of(
                new ArrayType(VARCHAR).getDisplayName(), new ArrayType(VARCHAR),
                StandardTypes.BIGINT, BIGINT,
                StandardTypes.TIMESTAMP, TIMESTAMP,
                StandardTypes.VARCHAR, VARCHAR);

        public TestingTypeDeserializer()
        {
            super(Type.class);
        }

        @Override
        protected Type _deserialize(String value, DeserializationContext context)
        {
            Type type = types.get(value.toLowerCase(ENGLISH));
            checkArgument(type != null, "Unknown type %s", value);
            return type;
        }
    }
}
