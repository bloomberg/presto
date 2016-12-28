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
import com.facebook.presto.spi.function.OperatorDependency;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;

@ScalarFunction("array_overlap")
@Description("Check whether elements of the two given arrays overlap")
public final class ArrayOverlapFunction
{
    private static final int INITIAL_SIZE = 128;

    private int[] positions = new int[INITIAL_SIZE];

    @TypeParameter("E")
    public ArrayOverlapFunction(@TypeParameter("E") Type elementType) {}

    private static IntComparator IntBlockCompare(Type type, Block block)
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
    public boolean overlap(
            @OperatorDependency(operator = LESS_THAN, returnType = StandardTypes.BOOLEAN, argumentTypes = {"E", "E"}) MethodHandle lessThanFunction,
            @TypeParameter("E") Type type,
            @SqlType("array(E)") Block leftArray,
            @SqlType("array(E)") Block rightArray)
    {
        int leftPositionCount = leftArray.getPositionCount();
        int rightPositionCount = rightArray.getPositionCount();

        if (leftPositionCount == 0) {
            return false;
        }

        if (rightPositionCount == 0) {
            return false;
        }

        if (leftPositionCount < rightPositionCount) {
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

        IntArrays.quickSort(positions, 0, positions.length, IntBlockCompare(type, leftArray));
        int currentPosition = 0;
        while (currentPosition < rightPositionCount) {
            int low = 0;
            int high = leftPositionCount - 1;
            while (high >= low) {
                int middle = (low + high) / 2;
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
            currentPosition++;
        }

        return false;
    }
}
