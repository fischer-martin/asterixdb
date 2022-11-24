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

    public JSONTreeJOFilterVisitor() {
        // Use ListObjectPool to reuse in-memory buffers instead of allocating new memory for each JOFilterNode.
        nodeAllocator = new ListObjectPool<>(new JOFilterNodeFactory());
    }

    public void reset() {
        // Free in-memory buffers for reuse.
        nodeAllocator.reset();
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

        // Create a new list node (array or multiset).
        JOFilterNode listNode = nodeAllocator.allocate(null);
        listNode.reset();
        listNode.setLabel(pointable);
        if (pointable.ordered()) {
            listNode.setType(4);
        } else {
            listNode.setType(5);
        }

        int subtreeSize = 1;
        // Recursively visit all list children (elements).
        for (int i = 0; i < pointable.getItems().size(); i++) {
            pointable.getItems().get(i).accept(this, arg);
            int currentChildPostorderedID = getPostorderIDFromArg(arg.right) - 1;

            listNode.addChild(currentChildPostorderedID);
            listNode.addSas(getSubtreeSizeFromArg(arg.right));
            listNode.setLeftSibling(leftSibling);
            subtreeSize += getSubtreeSizeFromArg(arg.right);
            maxChildHeight = Math.max(maxChildHeight, getSubtreeHeightFromArg(arg.right));

            // The favorable child is the child with the largest subtree.
            if (favChild == -1 || arg.left.get(currentChildPostorderedID).getSubtreeSize() > arg.left.get(favChild).getSubtreeSize()) {
                favChild = currentChildPostorderedID;
                favChildLeftSibling = leftSibling;
            }

            // update leftSibling for next sibling
            leftSibling = getPostorderIDFromArg(arg.right);
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

        // Create a new object node.
        JOFilterNode objectNode = nodeAllocator.allocate(null);
        objectNode.reset();
        objectNode.setLabel(pointable);
        objectNode.setType(3);

        int subtreeSize = 1;
        // Recursively visit all object children (key-value pairs).
        for (int i = 0; i < pointable.getFieldValues().size(); i++) {
            pointable.getFieldValues().get(i).accept(this, arg);

            JOFilterNode keyNodeChild = arg.left.get(getPostorderIDFromArg(arg.right) - 1);
            // key node only has one child => this child does not have a left sibling
            keyNodeChild.setLeftSibling(-1);
            // Set key node's postorder ID as the child's parent's postorder ID.
            keyNodeChild.setParent(getPostorderIDFromArg(arg.right));

            incrementSubtreeSizeInArg(arg.right); // Increment subtree size for key.
            incrementSubtreeHeightInArg(arg.right); // Increment height for key.

            // Building a key node.
            IVisitablePointable key = pointable.getFieldNames().get(i);
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
