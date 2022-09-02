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

import java.util.List;

import org.apache.asterix.om.pointables.AFlatValuePointable;
import org.apache.asterix.om.pointables.AListVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.visitor.IVisitablePointableVisitor;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.util.container.IObjectPool;
import org.apache.asterix.om.util.container.ListObjectPool;
import org.apache.asterix.runtime.evaluators.common.Node;
import org.apache.asterix.runtime.evaluators.common.NodeFactory;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import it.unimi.dsi.fastutil.ints.IntIntPair;

/**
 * Use {@link JSONTreeVisitor} to recursively traverse pointables and transform them into a JSON tree.
 * Example: JSONTreeVisitor jTreeVisitor = new JSONTreeVisitor();
 *          IVisitablePointable pointable = ...;
 *          pointable.accept(jTreeVisitor, arg);
 */

public class JSONTreeVisitor implements IVisitablePointableVisitor<Void, MutablePair<List<Node>, IntIntPair>> {
    private final IObjectPool<Node, ATypeTag> nodeAllocator;

    public JSONTreeVisitor() {
        // Use ListObjectPool to reuse in-memory buffers instead of allocating new memory for each node.
        nodeAllocator = new ListObjectPool<>(new NodeFactory());
    }

    public void reset() {
        // Free in-memory buffers for reuse.
        nodeAllocator.reset();
    }

    @Override
    public Void visit(AListVisitablePointable pointable, MutablePair<List<Node>, IntIntPair> arg)
            throws HyracksDataException {
        // Create a new list node (array or multiset).
        Node listNode = nodeAllocator.allocate(null);
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
            listNode.addChild(arg.right.firstInt() - 1);
            listNode.addSas(arg.right.secondInt());
            subtreeSize += arg.right.secondInt();
        }

        // Sort and sum up entries in the SAS array and set subtree size of the list node.
        listNode.sortAggregateSas();
        listNode.setSubtreeSize(subtreeSize);

        // Add new list node and update the postorder id and subtree size.
        arg.left.add(listNode);
        arg.right.first(arg.right.firstInt() + 1);
        arg.right.second(subtreeSize);

        return null;
    }

    @Override
    public Void visit(ARecordVisitablePointable pointable, MutablePair<List<Node>, IntIntPair> arg)
            throws HyracksDataException {
        // Create a new object node.
        Node objectNode = nodeAllocator.allocate(null);
        objectNode.reset();
        objectNode.setLabel(pointable);
        objectNode.setType(3);

        int subtreeSize = 1;
        // Recursively visit all object children (key-value pairs).
        for (int i = 0; i < pointable.getFieldValues().size(); i++) {
            pointable.getFieldValues().get(i).accept(this, arg);

            arg.right.second(arg.right.secondInt() + 1); // Increment subtree size for key.

            // Building a key node.
            IVisitablePointable key = pointable.getFieldNames().get(i);
            Node keyNode = nodeAllocator.allocate(null);
            keyNode.reset();
            keyNode.setLabel(key);
            keyNode.setType(2);
            keyNode.setSubtreeSize(arg.right.secondInt());
            keyNode.addChild(arg.right.firstInt() - 1); // A key always has exactly one child.
            keyNode.addSas(arg.right.secondInt() - 1);

            arg.left.add(keyNode);

            // Add child to object node.
            objectNode.addChild(arg.right.firstInt());
            objectNode.addSas(arg.right.secondInt());
            subtreeSize += arg.right.secondInt();

            // Raise postorder id after processing a key.
            arg.right.first(arg.right.firstInt() + 1);
        }

        // Sort and sum up entries in the SAS array and set subtree size of the list node.
        objectNode.sortAggregateSas();
        objectNode.setSubtreeSize(subtreeSize);

        // Add new list node and update the postorder id and subtree size.
        arg.left.add(objectNode);
        arg.right.first(arg.right.firstInt() + 1);
        arg.right.second(subtreeSize);

        return null;
    }

    @Override
    public Void visit(AFlatValuePointable pointable, MutablePair<List<Node>, IntIntPair> arg)
            throws HyracksDataException {
        arg.right.second(1); // The subtree size of a literal node is always 1 (the node itself).

        // Create a new literal node.
        Node literalNode = nodeAllocator.allocate(null);
        literalNode.reset();
        literalNode.setLabel(pointable);
        literalNode.setType(1);
        literalNode.setSubtreeSize(arg.right.secondInt());

        arg.left.add(literalNode);

        // Increase postorder id.
        arg.right.first(arg.right.firstInt() + 1);

        return null;
    }

}
