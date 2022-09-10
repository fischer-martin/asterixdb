/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.runtime.evaluators.functions.records;

import java.io.DataOutput;
import java.util.List;

import org.apache.asterix.builders.ArrayListFactory;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.pointables.PointableAllocator;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.evaluators.common.HungarianAlgorithm;
import org.apache.asterix.runtime.evaluators.common.JSONCostModel;
import org.apache.asterix.runtime.evaluators.common.JSONTreeTransformator;
import org.apache.asterix.runtime.evaluators.common.Node;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import it.unimi.dsi.fastutil.ints.IntIntMutablePair;
import it.unimi.dsi.fastutil.ints.IntIntPair;

public class JEDIEvaluator implements IScalarEvaluator {
    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput out = resultStorage.getDataOutput();
    protected final IScalarEvaluator firstStringEval;
    protected final IScalarEvaluator secondStringEval;
    protected final SourceLocation sourceLoc;
    protected final AMutableDouble aDouble = new AMutableDouble(-1.0);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AMutableDouble> doubleSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);

    private final IVisitablePointable pointableLeft;
    private final IVisitablePointable pointableRight;
    private final IObjectPool<List<Node>, ATypeTag> listAllocator;

    MutablePair<List<Node>, IntIntPair> transArg = new MutablePair<>();
    IntIntPair transCnt = new IntIntMutablePair(0, 0);
    private final JSONTreeTransformator treeTransformator = new JSONTreeTransformator();
    private final JSONCostModel cm = new JSONCostModel(1, 1, 1); // Unit cost model (each operation has cost 1).
    private double[][] treeDistanceMatrix; // JEDI tree distance matrix.
    private double[][] forestDistanceMatrix; // JEDI forest distance matrix.
    private double[][] editDistanceMatrix; // Distance matrix for sequence edit distance between ordered children.
    private double[][] hungarianMatrix; // Distance matrix for bipartite graph matching between unordered children.
    private double[] rowMinima;
    private double[] colMinima;
    private HungarianAlgorithm hungarianAlgo;

    // The following variables hold the insertion, deletion, and rename costs between the subtrees (resp.
    // subforests) of nodes i and j in t1 and t2.
    double forestInsertCost;
    double treeInsertCost;
    double forestDeleteCost;
    double treeDeleteCost;
    double forestRenameCost;
    double treeRenameCost;
    // The following variables hold upper and lower bounds on the rename costs.
    double forestUB;
    double aggregateSizeLB;
    int rowLB;
    int colLB;

    public JEDIEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context, SourceLocation sourceLoc,
            IAType type1, IAType type2) throws HyracksDataException {
        PointableAllocator allocator = new PointableAllocator();
        firstStringEval = args[0].createScalarEvaluator(context);
        secondStringEval = args[1].createScalarEvaluator(context);
        pointableLeft = allocator.allocateFieldValue(type1);
        pointableRight = allocator.allocateFieldValue(type2);
        listAllocator = new ListObjectPool<>(new ArrayListFactory<Node>());
        this.sourceLoc = sourceLoc;
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        resultStorage.reset();
        treeTransformator.reset();
        listAllocator.reset();
        firstStringEval.evaluate(tuple, pointableLeft);
        secondStringEval.evaluate(tuple, pointableRight);

        // Convert the given data items into JSON trees.
        List<Node> postToNode1 = listAllocator.allocate(null);
        postToNode1.clear();
        transArg.setLeft(postToNode1);
        transCnt.first(0);
        transCnt.second(0);
        transArg.setRight(transCnt);
        postToNode1 = treeTransformator.toTree(pointableLeft, transArg);

        List<Node> postToNode2 = listAllocator.allocate(null);
        postToNode2.clear();
        transArg.setLeft(postToNode2);
        transCnt.first(0);
        transCnt.second(0);
        transArg.setRight(transCnt);
        postToNode2 = treeTransformator.toTree(pointableRight, transArg);

        // Declare tree distance, forest distance, and string edit distance matrices.
        int sizeT1 = postToNode1.size();
        int sizeT2 = postToNode2.size();
        if (treeDistanceMatrix == null
                || (sizeT1 >= treeDistanceMatrix.length || sizeT2 >= treeDistanceMatrix[0].length)) {
            treeDistanceMatrix = new double[sizeT1 + 1][sizeT2 + 1];
            forestDistanceMatrix = new double[sizeT1 + 1][sizeT2 + 1];
            editDistanceMatrix = new double[sizeT1 + 1][sizeT2 + 1];
            rowMinima = new double[Math.max(sizeT1, sizeT2) + 1];
            colMinima = new double[Math.max(sizeT1, sizeT2) + 1];
            // Initialize Hungarian Algorithm matrix with maximum size of trees.
            hungarianAlgo = new HungarianAlgorithm(sizeT1 + sizeT2, sizeT1 + sizeT2);
            hungarianMatrix = new double[sizeT1 + sizeT2][sizeT1 + sizeT2];
        }

        writeResult(jedi(postToNode1, postToNode2));
        result.set(resultStorage);
    }

    protected void writeResult(double distance) throws HyracksDataException {
        aDouble.setValue(distance);
        doubleSerde.serialize(aDouble, out);
    }

    private double jedi(List<Node> t1, List<Node> t2) {
        int sizeT1 = t1.size();
        int sizeT2 = t2.size();

        // Initialize all distance matrices to infinity.
        for (int i = 0; i <= sizeT1; ++i) {
            for (int j = 0; j <= sizeT2; ++j) {
                treeDistanceMatrix[i][j] = Double.POSITIVE_INFINITY;
                forestDistanceMatrix[i][j] = Double.POSITIVE_INFINITY;
                editDistanceMatrix[i][j] = Double.POSITIVE_INFINITY;
            }
        }

        // Fill first row and first column.
        treeDistanceMatrix[0][0] = 0.0;
        forestDistanceMatrix[0][0] = 0.0;
        // Initialize deletion costs (first column).
        for (int i = 1; i <= sizeT1; ++i) {
            forestDistanceMatrix[i][0] = 0.0;
            for (int k = 1; k <= t1.get(i - 1).getChildren().size(); ++k) {
                forestDistanceMatrix[i][0] += treeDistanceMatrix[t1.get(i - 1).getChildren().getInt(k - 1) + 1][0];
            }
            treeDistanceMatrix[i][0] = forestDistanceMatrix[i][0] + cm.del(t1.get(i - 1));
        }
        // Initialize insertion costs (first row).
        for (int j = 1; j <= sizeT2; ++j) {
            forestDistanceMatrix[0][j] = 0.0;
            for (int k = 1; k <= t2.get(j - 1).getChildren().size(); ++k) {
                forestDistanceMatrix[0][j] += treeDistanceMatrix[0][t2.get(j - 1).getChildren().getInt(k - 1) + 1];
            }
            treeDistanceMatrix[0][j] = forestDistanceMatrix[0][j] + cm.ins(t2.get(j - 1));
        }

        // Fill the remaining fields in the forest and tree matrices to calculate the distances while iterating over
        // all node pairs between trees t1 and t2.
        for (int i = 1; i <= sizeT1; ++i) {
            for (int j = 1; j <= sizeT2; ++j) {
                // Compute the costs of all three edit operations and take the one with minimum cost.
                computeDeletionCosts(t1, i, j);
                computeInsertionCosts(t2, i, j);
                computeRenameCosts(t1, t2, i, j);

                // The forest and tree distance of nodes i and j is the minimum value among all three edit operations.
                forestDistanceMatrix[i][j] = Math.min(Math.min(forestDeleteCost, forestInsertCost), forestRenameCost);
                // In order to compute the tree rename cost, the forest distance cost must be known upfront. Add the
                // rename costs between i and j to their forest distance.
                treeRenameCost = forestDistanceMatrix[i][j] + cm.ren(t1.get(i - 1), t2.get(j - 1));
                treeDistanceMatrix[i][j] = Math.min(Math.min(treeDeleteCost, treeInsertCost), treeRenameCost);
            }
        }

        // The computed JEDI value is stored in the last row in the last column.
        return treeDistanceMatrix[sizeT1][sizeT2];
    }

    private void computeRenameCosts(List<Node> t1, List<Node> t2, int i, int j) {
        // Cost for renaming node i in t1 to node j in t2, i.e., find a minimum cost matching between i's and
        // j's children subtrees.
        forestRenameCost = Double.POSITIVE_INFINITY;
        // Cost for minimal mapping between children subtrees based on the node types.
        // In case of two keys, take the costs of mapping their child to one another.
        if (t1.get(i - 1).getType() == 2 && t2.get(j - 1).getType() == 2) {
            // Keys have exactly one child, therefore, [0] always works.
            forestRenameCost =
                    treeDistanceMatrix[t1.get(i - 1).getChildren().getInt(0) + 1][t2.get(j - 1).getChildren().getInt(0) + 1];
        }
        // Literals are leaves and hence have no subforest. Mapping their subforests has cost 0.
        else if (t1.get(i - 1).getType() == 1 && t2.get(j - 1).getType() == 1) {
            forestRenameCost = 0;
        } else {
            // Compute lower and upper bounds.
            forestUB = Math.min(forestInsertCost, forestDeleteCost);
            aggregateSizeLB = getAggregateSizeLB(t1, t2, i, j);

            // Only compute the expensive sibling matching if the aggregate size lower bound does not
            // exceed the forest cost upper bound.
            if (forestUB > aggregateSizeLB) {
                // If the nodes types are of type array, compute the sequence edit distance.
                if (t1.get(i - 1).getType() == 4 && t2.get(j - 1).getType() == 4) {
                    // Cost for renaming node i to node j, i.e., map the subtrees of i's children to the
                    // subtrees of j's children. The minimum mapping is computed with the sequence edit
                    // distance. In the tree distance matrix, the rename cost of nodes i and j are added.
                    editDistanceMatrix[0][0] = 0.0;
                    for (int s = 1; s <= t1.get(i - 1).getChildren().size(); ++s) {
                        editDistanceMatrix[s][0] = editDistanceMatrix[s - 1][0]
                                + treeDistanceMatrix[t1.get(i - 1).getChildren().getInt(s - 1) + 1][0];
                    }
                    for (int t = 1; t <= t2.get(j - 1).getChildren().size(); ++t) {
                        editDistanceMatrix[0][t] = editDistanceMatrix[0][t - 1]
                                + treeDistanceMatrix[0][t2.get(j - 1).getChildren().getInt(t - 1) + 1];
                    }

                    for (int s = 1; s <= t1.get(i - 1).getChildren().size(); ++s) {
                        for (int t = 1; t <= t2.get(j - 1).getChildren().size(); ++t) {
                            editDistanceMatrix[s][t] =
                                    Math.min(
                                            Math.min(
                                                    editDistanceMatrix[s][t - 1] + treeDistanceMatrix[0][t2.get(j - 1)
                                                            .getChildren().getInt(t - 1) + 1],
                                                    editDistanceMatrix[s - 1][t]
                                                            + treeDistanceMatrix[t1.get(i - 1).getChildren().getInt(s - 1)
                                                                    + 1][0]),
                                            editDistanceMatrix[s - 1][t - 1]
                                                    + treeDistanceMatrix[t1.get(i - 1).getChildren().getInt(s - 1) + 1][t2
                                                            .get(j - 1).getChildren().getInt(t - 1) + 1]);
                        }
                    }
                    // Assign string edit distance costs for subtree mapping cost.
                    forestRenameCost =
                            editDistanceMatrix[t1.get(i - 1).getChildren().size()][t2.get(j - 1).getChildren().size()];
                } else { // If the nodes types contain objects or multisets, compute the Hungarian Algorithm.
                    // Build a cost matrix such that each subtree can be mapped to another subtree or to
                    // an empty tree.
                    int hungarianMatrixSize = t1.get(i - 1).getChildren().size() + t2.get(j - 1).getChildren().size();

                    // Reset row and column minima.
                    for (int x = 0; x < hungarianMatrixSize; x++) {
                        rowMinima[x] = Double.POSITIVE_INFINITY;
                        colMinima[x] = Double.POSITIVE_INFINITY;
                    }

                    if (hungarianMatrixSize > 0) {
                        for (int s = 1; s <= hungarianMatrixSize; ++s) {
                            for (int t = 1; t <= hungarianMatrixSize; ++t) {
                                if (s <= t1.get(i - 1).getChildren().size()) {
                                    if (t <= t2.get(j - 1).getChildren().size()) {
                                        hungarianMatrix[s - 1][t - 1] =
                                                treeDistanceMatrix[t1.get(i - 1).getChildren().getInt(s - 1) + 1][t2
                                                        .get(j - 1).getChildren().getInt(t - 1) + 1];
                                        hungarianAlgo.costMatrix[s - 1][t - 1] =
                                                treeDistanceMatrix[t1.get(i - 1).getChildren().getInt(s - 1) + 1][t2
                                                        .get(j - 1).getChildren().getInt(t - 1) + 1];
                                    } else {
                                        hungarianMatrix[s - 1][t - 1] =
                                                treeDistanceMatrix[t1.get(i - 1).getChildren().getInt(s - 1) + 1][0];
                                        hungarianAlgo.costMatrix[s - 1][t - 1] =
                                                treeDistanceMatrix[t1.get(i - 1).getChildren().getInt(s - 1) + 1][0];
                                    }
                                } else {
                                    if (t <= t2.get(j - 1).getChildren().size()) {
                                        hungarianMatrix[s - 1][t - 1] =
                                                treeDistanceMatrix[0][t2.get(j - 1).getChildren().getInt(t - 1) + 1];
                                        hungarianAlgo.costMatrix[s - 1][t - 1] =
                                                treeDistanceMatrix[0][t2.get(j - 1).getChildren().getInt(t - 1) + 1];
                                    } else {
                                        hungarianMatrix[s - 1][t - 1] = 0.0;
                                        hungarianAlgo.costMatrix[s - 1][t - 1] = 0.0;
                                    }
                                }
                                rowMinima[s - 1] = Math.min(rowMinima[s - 1], hungarianAlgo.costMatrix[s - 1][t - 1]);
                                colMinima[t - 1] = Math.min(colMinima[t - 1], hungarianAlgo.costMatrix[s - 1][t - 1]);
                            }
                        }

                        // Compute lower bounds for rows and columns of the cost matrix of the
                        // Hungarian Algorithm.
                        rowLB = 0;
                        colLB = 0;
                        for (int x = 0; x < hungarianMatrixSize; x++) {
                            rowLB += rowMinima[x];
                            colLB += colMinima[x];
                        }

                        // Only compute the expensive bipartite graph matching if the local greedy lower
                        // bound does not exceed the forest cost upper bound.
                        if (forestUB > Math.max(rowLB, colLB)) {
                            // Compute Hungarian Algorithm for minimal subtree matching.
                            hungarianAlgo.init(hungarianMatrixSize, hungarianMatrixSize);
                            hungarianAlgo.execute();

                            forestRenameCost = 0;
                            for (int x = 0; x < hungarianMatrixSize; x++) {
                                if (hungarianAlgo.result[x] != -1) {
                                    forestRenameCost += hungarianMatrix[x][hungarianAlgo.result[x]];
                                }
                            }
                        }
                    } else {
                        forestRenameCost = 0;
                    }
                }
            }
        }
    }

    private void computeInsertionCosts(List<Node> t2, int i, int j) {
        // Cost for inserting node j in t2, i.e., map the subtree of node i in t1 to a subtree of j's children
        // with minimum cost.
        if (!t2.get(j - 1).getChildren().isEmpty()) {
            forestInsertCost = Double.POSITIVE_INFINITY;
            treeInsertCost = Double.POSITIVE_INFINITY;
            for (int t = 1; t <= t2.get(j - 1).getChildren().size(); ++t) {
                forestInsertCost =
                        Math.min(forestInsertCost, (forestDistanceMatrix[i][t2.get(j - 1).getChildren().getInt(t - 1) + 1]
                                - forestDistanceMatrix[0][t2.get(j - 1).getChildren().getInt(t - 1) + 1]));
                treeInsertCost =
                        Math.min(treeInsertCost, (treeDistanceMatrix[i][t2.get(j - 1).getChildren().getInt(t - 1) + 1]
                                - treeDistanceMatrix[0][t2.get(j - 1).getChildren().getInt(t - 1) + 1]));
            }
        } else { // In case node j in t2 has no children, delete the subtree of node i in t1.
            forestInsertCost = forestDistanceMatrix[i][0];
            treeInsertCost = treeDistanceMatrix[i][0];
        }
        forestInsertCost += forestDistanceMatrix[0][j];
        treeInsertCost += treeDistanceMatrix[0][j];
    }

    private void computeDeletionCosts(List<Node> t1, int i, int j) {
        // Cost for deleting node i in t1, i.e., map the subtree of node j in t2 to a subtree of i's children
        // with minimum cost.
        if (!t1.get(i - 1).getChildren().isEmpty()) {
            forestDeleteCost = Double.POSITIVE_INFINITY;
            treeDeleteCost = Double.POSITIVE_INFINITY;
            for (int s = 1; s <= t1.get(i - 1).getChildren().size(); ++s) {
                forestDeleteCost =
                        Math.min(forestDeleteCost, (forestDistanceMatrix[t1.get(i - 1).getChildren().getInt(s - 1) + 1][j]
                                - forestDistanceMatrix[t1.get(i - 1).getChildren().getInt(s - 1) + 1][0]));
                treeDeleteCost =
                        Math.min(treeDeleteCost, (treeDistanceMatrix[t1.get(i - 1).getChildren().getInt(s - 1) + 1][j]
                                - treeDistanceMatrix[t1.get(i - 1).getChildren().getInt(s - 1) + 1][0]));
            }
        } else { // In case node i in t1 has no children, delete the subtree of node j in t2.
            forestDeleteCost = forestDistanceMatrix[0][j];
            treeDeleteCost = treeDistanceMatrix[0][j];
        }
        forestDeleteCost += forestDistanceMatrix[i][0];
        treeDeleteCost += treeDistanceMatrix[i][0];
    }

    private double getAggregateSizeLB(List<Node> t1, List<Node> t2, int i, int j) {
        double aggregateSizeLB = 0; // Lower bound value.
        int k; // Children size difference.
        // In case that none of the two nodes has children, the aggregate size lower bound is 0.
        if (!t1.get(i - 1).getChildren().isEmpty() || !t2.get(j - 1).getChildren().isEmpty()) {
            k = Math.abs(t1.get(i - 1).getChildren().size() - t2.get(j - 1).getChildren().size());
            if (k == 0) {
                aggregateSizeLB += Math.abs(t1.get(i - 1).getSas().getInt(t1.get(i - 1).getChildren().size() - 1)
                        - t2.get(j - 1).getSas().getInt(t2.get(j - 1).getChildren().size() - 1));
            } else {
                // Node i in T1 has less or an equal number of children than node j in T2.
                if (t1.get(i - 1).getChildren().size() <= t2.get(j - 1).getChildren().size()) {
                    aggregateSizeLB = getSizeDiff(t1, t2, aggregateSizeLB, k, i, j);
                } else {
                    aggregateSizeLB = getSizeDiff(t2, t1, aggregateSizeLB, k, j, i);
                }
            }
        }
        return aggregateSizeLB;
    }

    private double getSizeDiff(List<Node> t1, List<Node> t2, double aggregateSizeLB, int k, int i, int j) {
        aggregateSizeLB += t2.get(j - 1).getSas().getInt(k - 1);
        if (!t1.get(i - 1).getChildren().isEmpty()) {
            aggregateSizeLB += Math.abs(t1.get(i - 1).getSas().getInt(t1.get(i - 1).getChildren().size() - 1)
                    - t2.get(j - 1).getSas().getInt(t2.get(j - 1).getChildren().size() - 1)
                    + t2.get(j - 1).getSas().getInt(k - 1));
        }
        return aggregateSizeLB;
    }
}
