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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;

@ScalarFunction("arrays_overlap")
@Description("Returns true if arrays have common elements")
public final class ArraysOverlapFunction
{
    @TypeParameter("E")
    public ArraysOverlapFunction(@TypeParameter("E") Type elementType) {}

    private static IntComparator intBlockCompare(Type type, Block block)
    {
        return new AbstractIntComparator()
        {
            @Override
            public int compare(int left, int right)
            {
                if (block.isNull(left) && block.isNull(right)) {
                    return 0;
                }
                if (block.isNull(left)) {
                    return -1;
                }
                if (block.isNull(right)) {
                    return 1;
                }
                return type.compareTo(block, left, block, right);
            }
        };
    }

    @TypeParameter("E")
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public Boolean arraysOverlap(
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        int leftPositionCount = leftArray.getPositionCount();
        int rightPositionCount = rightArray.getPositionCount();

        if (leftPositionCount == 0 || rightPositionCount == 0) {
            return false;
        }

        int[] positions;
        if (leftPositionCount >= rightPositionCount) {
            positions = new int[leftPositionCount];
        }
        else {
            positions = new int[rightPositionCount];
            Block temp = leftArray;
            leftArray = rightArray;
            rightArray = temp;
            leftPositionCount = leftArray.getPositionCount();
            rightPositionCount = rightArray.getPositionCount();
        }

        for (int i = 0; i < positions.length; i++) {
            positions[i] = i;
        }

        IntArrays.quickSort(positions, 0, positions.length, intBlockCompare(type, leftArray));

        // Get the location of the first non-null element from the long array
        int searchStart = getSearchStart(leftArray, positions);
        if (searchStart < 0) {
            return null;
        }

        boolean rightHasNull = false;
        for (int currentPosition = 0; currentPosition < rightPositionCount; ++currentPosition) {
            if (rightArray.isNull(currentPosition)) {
                rightHasNull = true;
                continue;
            }

            int low = searchStart;
            int high = leftPositionCount - 1;
            while (high >= low) {
                int middle = (low + high) / 2;
                if (leftArray.isNull(positions[middle])) {
                    break;
                }

                int compareValue = type.compareTo(leftArray, positions[middle], rightArray, currentPosition);
                if (compareValue > 0) {
                    high = middle - 1;
                }
                else if (compareValue < 0) {
                    low = middle + 1;
                }
                else {
                    return true;
                }
            }
        }

        if (rightHasNull || leftArray.isNull(positions[0])) {
            return null;
        }

        return false;
    }

    private int getSearchStart(Block leftArray, int[] positions)
    {
        for (int i = 0; i < positions.length; ++i) {
            if (!leftArray.isNull(positions[i])) {
                return i;
            }
        }
        return -1;
    }
}
