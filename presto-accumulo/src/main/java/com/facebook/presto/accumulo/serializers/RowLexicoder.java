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
package com.facebook.presto.accumulo.serializers;

import com.facebook.presto.accumulo.Types;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import org.apache.accumulo.core.client.lexicoder.Lexicoder;
import org.apache.accumulo.core.client.lexicoder.ListLexicoder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromArray;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromMap;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getBlockFromRow;
import static com.facebook.presto.accumulo.serializers.AccumuloRowSerializer.getRowFromBlock;
import static java.util.Objects.requireNonNull;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.concat;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.escape;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.split;
import static org.apache.accumulo.core.client.lexicoder.impl.ByteUtils.unescape;

public class RowLexicoder
        implements Lexicoder<Object>
{
    private static final byte[] EMPTY_BYTES = new byte[0];

    private List<Lexicoder> lexicoders;
    private Type rowType;

    public RowLexicoder(Type rowType, List<Lexicoder> lexicoders)
    {
        this.lexicoders = requireNonNull(lexicoders, "lexicoders is null");
        this.rowType = requireNonNull(rowType, "rowType is null");
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] encode(Object value)
    {
        List v;
        if (value instanceof Block) {
            v = getRowFromBlock(rowType, (Block) value);
        }
        else {
            v = (List) value;
        }

        byte[] nullElements = new byte[v.size()];
        for (int i = 0; i < v.size(); ++i) {
            nullElements[i] = v.get(i) == null ? (byte) 1 : 0;
        }

        byte[][] encElements = new byte[v.size() + 1][];
        encElements[0] = escape(nullElements);

        Iterator<Lexicoder> iterator = lexicoders.iterator();
        int i = 1;
        for (Object element : v) {
            Lexicoder lexicoder = iterator.next();
            if (element == null) {
                encElements[i] = escape(EMPTY_BYTES);
            }
            else if (lexicoder instanceof ListLexicoder) {
                Type elementType = Types.getElementType(rowType.getTypeParameters().get(i));
                encElements[i] = escape(lexicoder.encode(getBlockFromArray(elementType, (List<?>) element)));
            }
            else if (lexicoder instanceof MapLexicoder) {
                encElements[i] = escape(lexicoder.encode(getBlockFromMap(rowType.getTypeParameters().get(i), (Map<?, ?>) element)));
            }
            else if (lexicoder instanceof RowLexicoder) {
                encElements[i] = escape(lexicoder.encode(getBlockFromRow(rowType.getTypeParameters().get(i), (List<Object>) element)));
            }
            else {
                encElements[i] = escape(lexicoder.encode(element));
            }

            i++;
        }

        return concat(encElements);
    }

    @Override
    public List<?> decode(byte[] b)
    {
        byte[][] escapedElements = split(b, 0, b.length);
        List<Object> ret = new ArrayList<>(escapedElements.length);

        byte[] nullElements = unescape(escapedElements[0]);
        Iterator<Lexicoder> iterator = lexicoders.iterator();
        for (int i = 1; i < escapedElements.length; ++i) {
            Lexicoder lexicoder = iterator.next();
            if (nullElements[i - 1] == 0) {
                ret.add(lexicoder.decode(unescape(escapedElements[i])));
            }
            else {
                ret.add(null);
            }
        }

        return ret;
    }
}
