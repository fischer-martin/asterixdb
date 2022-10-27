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
package org.apache.asterix.runtime.evaluators.common;

import java.util.Collections;

import org.apache.hyracks.data.std.api.IPointable;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

/**
 * Represents a node in a JSON tree. Each nodes stores information in a label, its type (literal, key, object, array,
 * multiset), its subtree size, an array holding the id's of its children, and an array with the aggregate lower bound.
 * Example: Node n = new Node();
 */

public class Node {
    private IPointable label;
    private int type;
    private int subtreeSize;
    private final IntList children;
    private final IntList sas; // Sorted aggregate size.

    public Node() {
        this(null, -1, 1, new IntArrayList(), new IntArrayList());
    }

    public Node(IPointable label, int type) {
        this(label, type, 1, new IntArrayList(), new IntArrayList());
    }

    public Node(IPointable label, int type, int subtreeSize, IntList children, IntList childrenSizes) {
        this.label = label;
        this.type = type;
        this.subtreeSize = subtreeSize;
        this.children = children;
        this.sas = childrenSizes;
        sortAggregateSas();
    }

    public void reset() {
        this.children.clear();
        this.sas.clear();
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public IPointable getLabel() {
        return label;
    }

    public void setLabel(IPointable label) {
        this.label = label;
    }

    public int getSubtreeSize() {
        return subtreeSize;
    }

    public void setSubtreeSize(int subtreeSize) {
        this.subtreeSize = subtreeSize;
    }

    public IntList getChildren() {
        return children;
    }

    public void addChild(int child) {
        this.children.add(child);
    }

    public IntList getSas() {
        return sas;
    }

    public void addSas(int sas) {
        this.sas.add(sas);
    }

    public void sortAggregateSas() {
        Collections.sort(sas);
        if (sas.size() > 1) {
            for (int s = 1; s < sas.size(); s++) {
                sas.set(s, sas.getInt(s - 1) + sas.getInt(s));
            }
        }
    }
}
