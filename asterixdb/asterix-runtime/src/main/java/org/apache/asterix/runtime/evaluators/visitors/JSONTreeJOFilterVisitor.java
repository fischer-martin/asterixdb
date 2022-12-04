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
package org.apache.asterix.runtime.evaluators.visitors;

import org.apache.asterix.builders.ArrayListFactory;
import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.evaluators.common.JOFilterNode;
import org.apache.asterix.runtime.evaluators.common.JOFilterNodeFactory;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Use {@link JSONTreeJOFilterVisitor} to recursively traverse pointables and transform them into a JSON tree that is
 * suitable to be used to calculate the JOFilter.
 * Example: JSONTreeJOFilterVisitor jTreeJOFilterVisitor = new JSONTreeJOFilterVisitor();
 *          IVisitablePointable pointable = ...;
 *          pointable.accept(jTreeJOFilterVisitor, arg);
 */

public class JSONTreeJOFilterVisitor implements IVisitablePointableVisitor<Void, MutablePair<List<JOFilterNode>, int[]>> {
    private final IObjectPool<JOFilterNode, ATypeTag> nodeAllocator;
    private final IObjectPool<List<IVisitablePointable>, ATypeTag> listAllocator;
    private final OrderedJsonTreeIVisitablePointableComparator comparator = new OrderedJsonTreeIVisitablePointableComparator();

    public JSONTreeJOFilterVisitor() {
        // Use ListObjectPool to reuse in-memory buffers instead of allocating new memory for each JOFilterNode/List.
        nodeAllocator = new ListObjectPool<>(new JOFilterNodeFactory());
        listAllocator = new ListObjectPool<>(new ArrayListFactory());
    }

    public void reset() {
        // Free in-memory buffers for reuse.
        nodeAllocator.reset();
        listAllocator.reset();
    }

    /**
     * @param arg left: tree that is currently being built; right: 0: next postorder ID, 1: subtree size, 2: subtree height
     */
    @Override
    public Void visit(AListVisitablePointable pointable, MutablePair<List<JOFilterNode>, int[]> arg)
            throws HyracksDataException {
        int favChildLeftSibling = -1;
        int favChild = -1;
        int leftSibling = -1;
        int maxChildHeight = -1;
        List<IVisitablePointable> orderedChildren;

        // Create a new list node (array or multiset).
        JOFilterNode listNode = nodeAllocator.allocate(null);
        listNode.reset();
        listNode.setLabel(pointable);
        if (pointable.ordered()) {
            listNode.setType(4);
            orderedChildren = pointable.getItems();
        } else {
            listNode.setType(5);
            orderedChildren = listAllocator.allocate(null);
            orderedChildren.clear();
            orderedChildren.addAll(pointable.getItems());
            orderedChildren.sort(comparator);
        }

        int subtreeSize = 1;
        // Recursively visit all list children (elements).
        for (int i = 0; i < orderedChildren.size(); i++) {
            orderedChildren.get(i).accept(this, arg);
            int currentChildPostorderedID = getPostorderIDFromArg(arg.right) - 1;
            JOFilterNode currentChild = arg.left.get(currentChildPostorderedID);

            listNode.addChild(currentChildPostorderedID);
            listNode.addSas(getSubtreeSizeFromArg(arg.right));
            subtreeSize += getSubtreeSizeFromArg(arg.right);
            maxChildHeight = Math.max(maxChildHeight, getSubtreeHeightFromArg(arg.right));

            // The favorable child is the child with the largest subtree.
            if (favChild == -1 || currentChild.getSubtreeSize() > arg.left.get(favChild).getSubtreeSize()) {
                favChild = currentChildPostorderedID;
                favChildLeftSibling = leftSibling;
            }

            currentChild.setLeftSibling(leftSibling);

            // update leftSibling for next sibling
            leftSibling = currentChildPostorderedID;
        }

        if (!pointable.ordered()) {
            listAllocator.free(orderedChildren);
        }

        // Sort and sum up entries in the SAS array and set subtree size of the list node.
        listNode.sortAggregateSas();
        listNode.setSubtreeSize(subtreeSize);
        listNode.setHeight(maxChildHeight + 1);
        listNode.setFavChild(favChild);
        listNode.setFavChildLeftSibling(favChildLeftSibling);

        // Add new list node and update the postorder id and subtree size.
        arg.left.add(listNode);

        // For every one of its child nodes, set the list node's postorder ID as the parent's postorder ID.
        for (int i = 0; i < listNode.getChildren().size(); ++i) {
            arg.left.get(listNode.getChildren().getInt(i)).setParent(getPostorderIDFromArg(arg.right));
        }

        incrementPostorderIDInArg(arg.right);
        setSubtreeSizeInArg(arg.right, subtreeSize);
        setSubtreeHeightInArg(arg.right, maxChildHeight + 1);

        return null;
    }

    /**
     * @param arg left: tree that is currently being built; right: 0: next postorder ID, 1: subtree size, 2: subtree height
     */
    @Override
    public Void visit(ARecordVisitablePointable pointable, MutablePair<List<JOFilterNode>, int[]> arg)
            throws HyracksDataException {
        int leftSibling = -1;
        int favChildLeftSibling = -1;
        int favChild = -1;
        int maxChildHeight = -1;

        List<IVisitablePointable> orderedChildren = listAllocator.allocate(null);
        orderedChildren.clear();
        orderedChildren.addAll(pointable.getFieldNames());
        orderedChildren.sort(comparator);

        // Create a new object node.
        JOFilterNode objectNode = nodeAllocator.allocate(null);
        objectNode.reset();
        objectNode.setLabel(pointable);
        objectNode.setType(3);

        int subtreeSize = 1;
        // Recursively visit all object children (key-value pairs).
        for (int i = 0, valueIndex = -1; i < orderedChildren.size(); ++i) {
            // Since we reordered the keys, we need to find the index of the corresponding value.
            // We could save ourselves from these O(n^2) shenanigans if instead of using a List<IVisitablePointable> for
            // orderedChildren we used a List<MutablePair<IVisitablePointable, IVisitablePointable>> instead where the
            // left value would store the key and the right value would store the value and an additional Comparator
            // would wrap our existing OrderedJsonTreeIVisitablePointableComparator such that it compares based on the
            // keys only.
            // We don't do this since it would probably only really pay off for really large object instances and not
            // the ones that one would normally encounter in the real world.
            // TODO: Verify this if you want to perform micro optimizations.
            for (int k = 0; k < pointable.getFieldNames().size(); ++k) {
                if (orderedChildren.get(i) == pointable.getFieldNames().get(k)) {
                    valueIndex = k;
                    break;
                }
            }

            pointable.getFieldValues().get(valueIndex).accept(this, arg);

            JOFilterNode keyNodeChild = arg.left.get(getPostorderIDFromArg(arg.right) - 1);
            // key node only has one child => this child does not have a left sibling
            keyNodeChild.setLeftSibling(-1);
            // Set key node's postorder ID as the child's parent's postorder ID.
            keyNodeChild.setParent(getPostorderIDFromArg(arg.right));

            incrementSubtreeSizeInArg(arg.right); // Increment subtree size for key.
            incrementSubtreeHeightInArg(arg.right); // Increment height for key.

            // Building a key node.
            IVisitablePointable key = orderedChildren.get(i);
            JOFilterNode keyNode = nodeAllocator.allocate(null);
            keyNode.reset();
            keyNode.setLabel(key);
            keyNode.setType(2);
            keyNode.setSubtreeSize(getSubtreeSizeFromArg(arg.right));
            keyNode.addChild(getPostorderIDFromArg(arg.right) - 1); // A key always has exactly one child.
            keyNode.addSas(getSubtreeSizeFromArg(arg.right) - 1);
            keyNode.setHeight(getSubtreeHeightFromArg(arg.right));
            keyNode.setFavChild(getPostorderIDFromArg(arg.right) - 1); // One child => child is fav child
            keyNode.setFavChildLeftSibling(-1); // One child => child has no left sibling
            keyNode.setLeftSibling(leftSibling);

            // The favorable child is the child with the largest subtree.
            if (favChild == -1 || keyNode.getSubtreeSize() > arg.left.get(favChild).getSubtreeSize()) {
                favChild = getPostorderIDFromArg(arg.right);
                favChildLeftSibling = leftSibling;
            }

            arg.left.add(keyNode);

            // Add child to object node.
            objectNode.addChild(getPostorderIDFromArg(arg.right));
            objectNode.addSas(getSubtreeSizeFromArg(arg.right));
            subtreeSize += getSubtreeSizeFromArg(arg.right);
            maxChildHeight = Math.max(maxChildHeight, getSubtreeHeightFromArg(arg.right));

            // update leftSibling for next sibling
            leftSibling = getPostorderIDFromArg(arg.right);

            // Raise postorder id after processing a key.
            incrementPostorderIDInArg(arg.right);
        }

        listAllocator.free(orderedChildren);

        // Sort and sum up entries in the SAS array and set subtree size of the list node.
        objectNode.sortAggregateSas();
        objectNode.setSubtreeSize(subtreeSize);
        objectNode.setHeight(maxChildHeight + 1);
        objectNode.setFavChild(favChild);
        objectNode.setFavChildLeftSibling(favChildLeftSibling);

        // Add new list node and update the postorder id and subtree size.
        arg.left.add(objectNode);

        // For every one of its key nodes, set the object node's postorder ID as the parent's postorder ID.
        for (int i = 0; i < objectNode.getChildren().size(); ++i) {
            arg.left.get(objectNode.getChildren().getInt(i)).setParent(getPostorderIDFromArg(arg.right));
        }

        incrementPostorderIDInArg(arg.right);
        setSubtreeSizeInArg(arg.right, subtreeSize);
        setSubtreeHeightInArg(arg.right, objectNode.getHeight());

        return null;
    }

    /**
     * @param arg left: tree that is currently being built; right: 0: next postorder ID, 1: subtree size, 2: subtree height
     */
    @Override
    public Void visit(AFlatValuePointable pointable, MutablePair<List<JOFilterNode>, int[]> arg)
            throws HyracksDataException {
        setSubtreeSizeInArg(arg.right, 1); // The subtree size of a literal node is always 1 (the node itself).
        setSubtreeHeightInArg(arg.right, 0); // The height of a (sub)tree with one node is always 0;

        // Create a new literal node.
        JOFilterNode literalNode = nodeAllocator.allocate(null);
        literalNode.reset();
        literalNode.setLabel(pointable);
        literalNode.setType(1);
        literalNode.setSubtreeSize(getSubtreeSizeFromArg(arg.right));
        literalNode.setHeight(getSubtreeHeightFromArg(arg.right));
        // literal node has no children
        literalNode.setFavChild(-1);
        literalNode.setFavChildLeftSibling(-1);

        arg.left.add(literalNode);

        // Increase postorder id.
        incrementPostorderIDInArg(arg.right);

        return null;
    }

    /**
     * Note: this comparator imposes orderings that are inconsistent with equals.
     */
    private class OrderedJsonTreeIVisitablePointableComparator implements Comparator<IVisitablePointable> {

        private static final int MULTISET = 4;
        private static final int ARRAY = 3;
        private static final int OBJECT = 2;
        private static final int KEY_OR_LITERAL = 1;

        /**
         * Implies an order where multiset > array > object > key/literal. The keys/literals are subordered through a
         * lexicographic order that first compares the type and then the data itself.
         *
         * @param o1 first argument for comparison
         * @param o2 second argument for comparison
         * @return A negative integer, zero, or a positive integer if o1 is less than, equal to, or greater than o2.
         */
        @Override
        public int compare(IVisitablePointable o1, IVisitablePointable o2) {
            final int o1Type = determineType(o1);
            final int o2Type = determineType(o2);

            if (o1Type == KEY_OR_LITERAL && o2Type == KEY_OR_LITERAL) {
                AFlatValuePointable first = (AFlatValuePointable) o1;
                AFlatValuePointable second = (AFlatValuePointable) o2;

                return Arrays.compare(first.getByteArray(), first.getStartOffset(), first.getStartOffset() + first.getLength(),
                        second.getByteArray(), second.getStartOffset(), second.getStartOffset() + second.getLength());
            } else {
                return o1Type - o2Type;
            }
        }

        private int determineType(IVisitablePointable p) {
            if (p instanceof AListVisitablePointable) {
                if (((AListVisitablePointable) p).ordered()) {
                    return ARRAY;
                } else {
                    return MULTISET;
                }
            } else if (p instanceof ARecordVisitablePointable) {
                return OBJECT;
            } else {
                return KEY_OR_LITERAL;
            }
        }

    }

    private int getPostorderIDFromArg(int[] arg) {
        return arg[0];
    }

    private void setPostorderIDInArg(int[] arg, int postorderID) {
        arg[0] = postorderID;
    }

    private void incrementPostorderIDInArg(int[] arg) {
        setPostorderIDInArg(arg, getPostorderIDFromArg(arg) + 1);
    }

    private int getSubtreeSizeFromArg(int[] arg) {
        return arg[1];
    }

    private void setSubtreeSizeInArg(int[] arg, int subtreeSize) {
        arg[1] = subtreeSize;
    }

    private void incrementSubtreeSizeInArg(int[] arg) {
        setSubtreeSizeInArg(arg, getSubtreeSizeFromArg(arg) + 1);
    }

    private int getSubtreeHeightFromArg(int[] arg) {
        return arg[2];
    }

    private void setSubtreeHeightInArg(int[] arg, int subtreeHeight) {
        arg[2] = subtreeHeight;
    }

    private void incrementSubtreeHeightInArg(int[] arg) {
        setSubtreeHeightInArg(arg, getSubtreeHeightFromArg(arg) + 1);
    }

}
