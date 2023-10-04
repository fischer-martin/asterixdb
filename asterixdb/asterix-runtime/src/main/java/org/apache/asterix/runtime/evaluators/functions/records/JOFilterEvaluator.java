/*
 * The MIT License (MIT)
 * Copyright (c) 2021 Thomas Huetter.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.apache.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;
import java.util.Arrays;
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.serde.*;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.common.*;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

/*
 * This class is based on the JOFilter's reference implementation as presented in the 'tree-similarity' repository
 * (https://github.com/DatabaseGroup/tree-similarity) maintained by the Database Research Group at the University of
 * Salzburg. The original implementation can be found at
 * https://github.com/DatabaseGroup/tree-similarity/blob/19bab382d968464ff53bd63f8e12e83d941c9430/src/json/jofilter_index_impl.h.
 *
 * Significant changes are:
 * - a port of the implementation from C++ to Java and
 * - changes necessary to make the implementation work/fit better into the AsterixDB ecosystem. These include (but are not limited to):
 *      - Building (and organizing) trees in a fashion similiar to the already existing mechanism for building JSON trees and
 *      - reusing data structures whenever possible.
 */

public class JOFilterEvaluator implements IScalarEvaluator {
    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput out = resultStorage.getDataOutput();
    protected final IScalarEvaluator firstStringEval;
    protected final IScalarEvaluator secondStringEval;
    protected final IScalarEvaluator thirdStringEval;
    protected final IEvaluatorContext context;
    protected final SourceLocation sourceLoc;
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<ABoolean> booleanSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);

    private final IVisitablePointable pointableLeft;
    private final IVisitablePointable pointableRight;
    protected final IPointable thresholdPointable = new VoidPointable();

    private MutablePair<JOFilterTree, int[]> transArg = new MutablePair<>();
    private int[] transCnt = new int[3];
    private JOFilterTree t1 = new JOFilterTree();
    private JOFilterTree t2 = new JOFilterTree();
    private final JSONTreeTransformator treeTransformator = new JSONTreeTransformator();
    private final JSONCostModel cm = new JSONCostModel(1, 1, 1); // Unit cost model (each operation has cost 1).

    // Costs to delete T1[i].
    private double[] delT1Subtree;
    // Costs to delete F1[i].
    private double[] delF1Subtree;
    // Costs to insert T2[j].
    private double[] insT2Subtree;
    // Costs to insert F2[j].
    private double[] insF2Subtree;
    // Holds deletion line for the edit distance computation.
    private double[] eInit;
    // Tree distance matrix, initialized to infinity.
    private double[][] treeDistanceMatrix;
    // Forest distance matrix, initialized to infinity.
    private double[][] forestDistanceMatrix;
    // Holds line c(s-1)(t) for the edit distance computation.
    private double[][] editDistanceMatrix0;
    // Holds line c(s)(t) for the edit distance computation.
    private double[][] editDistanceMatrix;
    // Iteratively compute forest deletion.
    private double[][] forestDeletionMatrix;
    // Holds line c(s)(t) for the edit distance computation.
    private double[][] treeDeletionMatrix;
    // Holds the distance matrix line of the favorable child.
    private double[][] favorableChildTreeDistanceMatrix;

    public JOFilterEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context, SourceLocation sourceLoc,
            IAType type1, IAType type2) throws HyracksDataException {
        PointableAllocator allocator = new PointableAllocator();
        firstStringEval = args[0].createScalarEvaluator(context);
        secondStringEval = args[1].createScalarEvaluator(context);
        thirdStringEval = args[2].createScalarEvaluator(context);
        pointableLeft = allocator.allocateFieldValue(type1);
        pointableRight = allocator.allocateFieldValue(type2);
        this.context = context;
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        treeTransformator.reset();
        firstStringEval.evaluate(tuple, pointableLeft);
        secondStringEval.evaluate(tuple, pointableRight);
        thirdStringEval.evaluate(tuple, thresholdPointable);

        if (PointableHelper.checkAndSetMissingOrNull(result, thresholdPointable)) {
            return;
        }

        byte[] data = thresholdPointable.getByteArray();
        int offset = thresholdPointable.getStartOffset();
        double threshold;
        if (data[offset] == ATypeTag.SERIALIZED_INT8_TYPE_TAG) {
            threshold = AInt8SerializerDeserializer.getByte(data, offset + 1);
        } else if (data[offset] == ATypeTag.SERIALIZED_INT16_TYPE_TAG) {
            threshold = AInt16SerializerDeserializer.getShort(data, offset + 1);
        } else if (data[offset] == ATypeTag.SERIALIZED_INT32_TYPE_TAG) {
            threshold = AInt32SerializerDeserializer.getInt(data, offset + 1);
        } else if (data[offset] == ATypeTag.SERIALIZED_INT64_TYPE_TAG) {
            threshold = AInt64SerializerDeserializer.getLong(data, offset + 1);
        } else if (data[offset] == ATypeTag.SERIALIZED_FLOAT_TYPE_TAG) {
            threshold = AFloatSerializerDeserializer.getFloat(data, offset + 1);
        } else if (data[offset] == ATypeTag.SERIALIZED_DOUBLE_TYPE_TAG) {
            threshold = ADoubleSerializerDeserializer.getDouble(data, offset + 1);
        } else {
            ExceptionUtil.warnTypeMismatch(context, sourceLoc, JOFilterDescriptor.getIdentifierStatic(), data[offset],
                    2, ArgumentUtils.NUMERIC_TYPES);
            PointableHelper.setNull(result);
            return;
        }
        if (threshold < 0) {
            ExceptionUtil.warnNegativeValue(context, sourceLoc, JOFilterDescriptor.getIdentifierStatic(), 2, threshold);
            PointableHelper.setNull(result);
            return;
        }

        // Convert the given data items into JSON trees.
        convertToJOFilterTree(pointableLeft, t1);
        convertToJOFilterTree(pointableRight, t2);

        writeResult(joFilter(t1, t2, threshold));
        result.set(resultStorage);
    }

    protected void writeResult(boolean withinThreshold) throws HyracksDataException {
        booleanSerde.serialize(ABoolean.valueOf(withinThreshold), out);
    }

    private JOFilterTree convertToJOFilterTree(IVisitablePointable pointable, JOFilterTree tree)
            throws HyracksDataException {
        tree.reset();
        transArg.setLeft(tree);
        transCnt[0] = 0;
        transCnt[1] = 0;
        transCnt[2] = 0;
        transArg.setRight(transCnt);

        return treeTransformator.toJOFilterTree(pointable, transArg);
    }

    private void resizeDataStructures(int sizeT1, int heightT1, int sizeT2) {
        // Resize data structures iff they are too small in order to not
        // create too much unnecessary garbage that needs to be collected.
        if (delT1Subtree == null || delT1Subtree.length < sizeT1 + 1) {
            delT1Subtree = new double[sizeT1 + 1];
            delF1Subtree = new double[sizeT1 + 1];
        }
        if (insT2Subtree == null || insT2Subtree.length < sizeT2 + 1) {
            insT2Subtree = new double[sizeT2 + 1];
            insF2Subtree = new double[sizeT2 + 1];
            eInit = new double[sizeT2 + 1];
        }
        if (treeDistanceMatrix == null || treeDistanceMatrix.length < heightT1 + 1
                || treeDistanceMatrix[0].length < sizeT2 + 1) {
            treeDistanceMatrix = new double[heightT1 + 1][sizeT2 + 1];
            forestDistanceMatrix = new double[heightT1 + 1][sizeT2 + 1];
            editDistanceMatrix0 = new double[heightT1 + 1][sizeT2 + 1];
            editDistanceMatrix = new double[heightT1 + 1][sizeT2 + 1];
            forestDeletionMatrix = new double[heightT1 + 1][sizeT2 + 1];
            treeDeletionMatrix = new double[heightT1 + 1][sizeT2 + 1];
            favorableChildTreeDistanceMatrix = new double[heightT1 + 1][sizeT2 + 1];
        }
    }

    private void initializeDataStructures(List<JOFilterNode> t1, List<JOFilterNode> t2) {
        int sizeT1 = t1.size();
        int heightT1 = t1.get(sizeT1 - 1).getHeight();
        int sizeT2 = t2.size();

        // Fill the matrices with infinity.
        for (int i = 0; i < heightT1 + 1; ++i) {
            Arrays.fill(treeDistanceMatrix[i], 0, sizeT2 + 1, Double.POSITIVE_INFINITY);
            Arrays.fill(forestDistanceMatrix[i], 0, sizeT2 + 1, Double.POSITIVE_INFINITY);
            Arrays.fill(editDistanceMatrix0[i], 0, sizeT2 + 1, Double.POSITIVE_INFINITY);
            Arrays.fill(editDistanceMatrix[i], 0, sizeT2 + 1, Double.POSITIVE_INFINITY);
            Arrays.fill(treeDeletionMatrix[i], 0, sizeT2 + 1, Double.POSITIVE_INFINITY);
            Arrays.fill(forestDeletionMatrix[i], 0, sizeT2 + 1, Double.POSITIVE_INFINITY);
            Arrays.fill(favorableChildTreeDistanceMatrix[i], 0, sizeT2 + 1, Double.POSITIVE_INFINITY);
        }

        // Initialize the cost matrices.
        delT1Subtree[0] = 0;
        delF1Subtree[0] = 0;
        for (int i = 1; i <= sizeT1; ++i) {
            delF1Subtree[i] = 0;
            for (int k = 1; k <= t1.get(i - 1).getChildren().size(); ++k) {
                delF1Subtree[i] += delT1Subtree[t1.get(i - 1).getChildren().getInt(k - 1) + 1];
            }
            delT1Subtree[i] = delF1Subtree[i] + cm.del(t1.get(i - 1));
        }

        insT2Subtree[0] = 0;
        insF2Subtree[0] = 0;
        eInit[0] = 0;
        for (int j = 1; j <= sizeT2; ++j) {
            insF2Subtree[j] = 0;
            for (int k = 1; k <= t2.get(j - 1).getChildren().size(); ++k) {
                insF2Subtree[j] += insT2Subtree[t2.get(j - 1).getChildren().getInt(k - 1) + 1];
                if (k == 1) {
                    // TODO: could be moved out of the inner loop if it is placed in an
                    // if (t2.get(j - 1).getChildren().size() >= 1) so that we don't have this case distinction in here
                    eInit[t2.get(j - 1).getChildren().getInt(k - 1) + 1] =
                            eInit[0] + insT2Subtree[t2.get(j - 1).getChildren().getInt(k - 1) + 1];
                } else {
                    eInit[t2.get(j - 1).getChildren().getInt(k - 1) + 1] =
                            eInit[t2.get(j - 1).getChildren().getInt(k - 2) + 1]
                                    + insT2Subtree[t2.get(j - 1).getChildren().getInt(k - 1) + 1];
                }
            }
            insT2Subtree[j] = insF2Subtree[j] + cm.ins(t2.get(j - 1));
        }
        eInit[sizeT2] = sizeT2;
    }

    private double joFilterCalculation(JOFilterTree t1, JOFilterTree t2, double threshold) {
        List<JOFilterNode> t1List = t1.getPostorderedTree();
        List<JOFilterNode> t2List = t2.getPostorderedTree();
        int sizeT1 = t1List.size();
        int sizeT2 = t2List.size();
        // Stop if one of the trees is empty.
        if (sizeT1 < 1) {
            return sizeT2;
        } else if (sizeT2 < 1) {
            return sizeT1;
        }
        int heightT1 = t1List.get(sizeT1 - 1).getHeight();

        resizeDataStructures(sizeT1, heightT1, sizeT2);
        initializeDataStructures(t1List, t2List);

        // Store minimum costs for each operation.
        double minForestIns = Double.POSITIVE_INFINITY;
        double minTreeIns = Double.POSITIVE_INFINITY;
        double minForestDel = Double.POSITIVE_INFINITY;
        double minTreeDel = Double.POSITIVE_INFINITY;
        double minForestRen = Double.POSITIVE_INFINITY;
        double minTreeRen = Double.POSITIVE_INFINITY;
        int i; // Postorder number of currently processed node.
        int parentI; // Line in the matrix for parent of node i.
        int nodeJThresholdRangeStart; // Start of threshold range for node j.
        int nodeJThresholdRangeEnd; // End of threshold range for node j.
        int favChildPostorderID; // Holds the favorable child of the current node.
        for (int x = 1; x <= sizeT1; ++x) {
            // Get postorder number from favorable child order number.
            i = t1.favChildOrderToPostorder(x - 1) + 1;

            // Iterate for all j in the threshold range of i.
            nodeJThresholdRangeStart = Math.max(i - (int) threshold, 1);
            nodeJThresholdRangeEnd = Math.min(i + (int) threshold, sizeT2);
            for (int j = nodeJThresholdRangeStart; j <= nodeJThresholdRangeEnd; ++j) {
                // Cost for deletion.
                // Must be set to infinity, since we allow infinity costs for different node types.
                minForestDel = Double.POSITIVE_INFINITY;
                minTreeDel = Double.POSITIVE_INFINITY;
                if (t1List.get(i - 1).getChildren().size() != 0) {
                    // t1List[i] is not a leaf node. Therefore, read the previously computed value.
                    minForestDel = forestDeletionMatrix[t1List.get(i - 1).getHeight()][j - 1];
                    minTreeDel = treeDeletionMatrix[t1List.get(i - 1).getHeight()][j - 1];
                }

                // Cost for insertion.
                minForestIns = Double.POSITIVE_INFINITY;
                minTreeIns = Double.POSITIVE_INFINITY;
                for (int t = 0; t < t2List.get(j - 1).getChildren().size(); ++t) {
                    if (Math.abs(i - (t2List.get(j - 1).getChildren().getInt(t) + 1)) > threshold) {
                        continue;
                    }
                    minForestIns =
                            Math.min(minForestIns,
                                    (forestDistanceMatrix[t1List.get(i - 1).getHeight()][t2List.get(j - 1).getChildren()
                                            .getInt(t) + 1]
                                            - insF2Subtree[t2List.get(j - 1).getChildren().getInt(t) + 1]));
                    minTreeIns =
                            Math.min(minTreeIns,
                                    (treeDistanceMatrix[t1List.get(i - 1).getHeight()][t2List.get(j - 1).getChildren()
                                            .getInt(t) + 1]
                                            - insT2Subtree[t2List.get(j - 1).getChildren().getInt(t) + 1]));
                }
                minForestIns += insF2Subtree[j];
                minTreeIns += insT2Subtree[j];

                // Cost for rename.
                if (t1List.get(i - 1).getChildren().size() == 0) {
                    // t1List[i] is a leaf node. Therefore, all nodes of F2 have to be inserted.
                    minForestRen = insF2Subtree[j];
                } else if (t2List.get(j - 1).getChildren().size() == 0) {
                    // t2List[j] is a leaf node. Therefore, all nodes of F1 have to be deleted.
                    minForestRen = delF1Subtree[i];
                } else {
                    minForestRen = editDistanceMatrix0[t1List.get(i - 1).getHeight()][t2List.get(j - 1).getChildren()
                            .getInt(t2List.get(j - 1).getChildren().size() - 1) + 1];
                }

                // Fill forest distance matrix.
                forestDistanceMatrix[t1List.get(i - 1).getHeight()][j] =
                        Math.min(Math.min(minForestDel, minForestIns), minForestRen);
                // Compute tree rename based on forest cost matrix.
                minTreeRen = forestDistanceMatrix[t1List.get(i - 1).getHeight()][j]
                        + cm.ren(t1List.get(i - 1), t2List.get(j - 1));

                // Fill tree distance matrix.
                treeDistanceMatrix[t1List.get(i - 1).getHeight()][j] =
                        Math.min(Math.min(minTreeDel, minTreeIns), minTreeRen);

                // Do not compute for the parent of the root node in T1.
                if (i != sizeT1) {
                    parentI = t1List.get(t1List.get(i - 1).getParent()).getHeight();
                    // Case 1: i is favorable child of parent.
                    if (t1List.get(t1List.get(i - 1).getParent()).getFavChild() == i - 1) {
                        // Store distances for favorable child used to fill the edit distance matrix later on.
                        favorableChildTreeDistanceMatrix[parentI][j] =
                                treeDistanceMatrix[t1List.get(i - 1).getHeight()][j];
                        // Keep track of the deletion costs for parent.
                        forestDeletionMatrix[parentI][j - 1] = delF1Subtree[t1List.get(i - 1).getParent() + 1]
                                + forestDistanceMatrix[t1List.get(i - 1).getHeight()][j] - delF1Subtree[i];
                        treeDeletionMatrix[parentI][j - 1] = delT1Subtree[t1List.get(i - 1).getParent() + 1]
                                + treeDistanceMatrix[t1List.get(i - 1).getHeight()][j] - delT1Subtree[i];
                    } else {
                        // Keep track of the deletion costs for parent.
                        forestDeletionMatrix[parentI][j - 1] =
                                Math.min(forestDeletionMatrix[parentI][j - 1],
                                        delF1Subtree[t1List.get(i - 1).getParent() + 1]
                                                + forestDistanceMatrix[t1List.get(i - 1).getHeight()][j]
                                                - delF1Subtree[i]);
                        treeDeletionMatrix[parentI][j - 1] =
                                Math.min(treeDeletionMatrix[parentI][j - 1],
                                        delT1Subtree[t1List.get(i - 1).getParent() + 1]
                                                + treeDistanceMatrix[t1List.get(i - 1).getHeight()][j]
                                                - delT1Subtree[i]);
                    }

                    // Do not store the edit matrix for the parent of the root node.
                    if (j != sizeT2) {
                        // Case 2.1: i is the leftmost child, hence take init line.
                        if (t1List.get(t1List.get(i - 1).getParent()).getChildren().size() > 0
                                && t1List.get(t1List.get(i - 1).getParent()).getChildren().getInt(0) == i - 1) {
                            // Fill next line.
                            editDistanceMatrix[parentI][0] = eInit[0] + delT1Subtree[i];
                            // If the current node j in tree t2List is (the root node or) the first child start from empty column (0).
                            editDistanceMatrix[parentI][j] = Math.min(
                                    editDistanceMatrix[parentI][t2List.get(j - 1).getLeftSibling() + 1]
                                            + insT2Subtree[j],
                                    Math.min(eInit[j] + delT1Subtree[i], eInit[t2List.get(j - 1).getLeftSibling() + 1]
                                            + treeDistanceMatrix[t1List.get(i - 1).getHeight()][j]));
                        }
                        // Case 2.2: i is not the leftmost child, hence take previous line.
                        else if (t1List.get(t1List.get(i - 1).getParent()).getFavChild() != i - 1) {
                            // Fill next line.
                            editDistanceMatrix[parentI][0] = editDistanceMatrix0[parentI][0] + delT1Subtree[i];
                            // If the current node j in tree t2List is (the root node or) the first child start from empty column (0).
                            editDistanceMatrix[parentI][j] = Math.min(
                                    editDistanceMatrix[parentI][t2List.get(j - 1).getLeftSibling() + 1]
                                            + insT2Subtree[j],
                                    Math.min(editDistanceMatrix0[parentI][j] + delT1Subtree[i],
                                            editDistanceMatrix0[parentI][t2List.get(j - 1).getLeftSibling() + 1]
                                                    + treeDistanceMatrix[t1List.get(i - 1).getHeight()][j]));
                        }
                    }
                }
            }
            // Case 3: t[i] is the left sibling of the favorable child.
            if (i != sizeT1) {
                parentI = t1List.get(t1List.get(i - 1).getParent()).getHeight();
                if (t1List.get(t1List.get(i - 1).getParent()).getFavChildLeftSibling() == i - 1) {
                    favChildPostorderID = t1List.get(t1List.get(i - 1).getParent()).getFavChild() + 1;

                    for (int p = 0; p <= sizeT2; p++) {
                        editDistanceMatrix0[parentI][p] = Double.POSITIVE_INFINITY;
                    }
                    editDistanceMatrix0[parentI][0] =
                            editDistanceMatrix[parentI][0] + delT1Subtree[favChildPostorderID];

                    nodeJThresholdRangeStart = Math.max(favChildPostorderID - (int) threshold, 1);
                    nodeJThresholdRangeEnd = Math.min(favChildPostorderID + (int) threshold, sizeT2);
                    for (int j = nodeJThresholdRangeStart; j <= nodeJThresholdRangeEnd; ++j) {
                        editDistanceMatrix0[parentI][j] = Math.min(
                                editDistanceMatrix0[parentI][t2List.get(j - 1).getLeftSibling() + 1] + insT2Subtree[j],
                                Math.min(editDistanceMatrix[parentI][j] + delT1Subtree[favChildPostorderID],
                                        editDistanceMatrix[parentI][t2List.get(j - 1).getLeftSibling() + 1]
                                                + favorableChildTreeDistanceMatrix[parentI][j]));
                    }
                } else {
                    System.arraycopy(editDistanceMatrix[parentI], 0, editDistanceMatrix0[parentI], 0, sizeT2 + 1);
                }
                // Reset data structures to infinity.
                for (int p = 0; p <= sizeT2; p++) {
                    editDistanceMatrix[parentI][p] = Double.POSITIVE_INFINITY;
                    treeDistanceMatrix[t1List.get(i - 1).getHeight()][p] = Double.POSITIVE_INFINITY;
                    forestDistanceMatrix[t1List.get(i - 1).getHeight()][p] = Double.POSITIVE_INFINITY;
                }
            }
        }

        return treeDistanceMatrix[heightT1][sizeT2];
    }

    private boolean joFilter(JOFilterTree t1, JOFilterTree t2, double threshold) {
        return joFilterCalculation(t1, t2, threshold) <= threshold;
    }

}
