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

import java.util.HashMap;
import java.util.List;

import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.runtime.evaluators.visitors.JSONTreeLabelTypeIntersectionVisitor;
import org.apache.asterix.runtime.evaluators.visitors.JSONTreeSizeVisitor;
import org.apache.asterix.runtime.evaluators.visitors.JSONTreeVisitor;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import it.unimi.dsi.fastutil.ints.IntIntPair;

/**
 * Use {@link JSONTreeTransformator} to transform any pointable value, including ordered and unordered lists, record
 * values, etc. into an according JSON tree.
 * Example: Let IVisitablePointable pointable be a value reference. To transform them into JSON trees, use
 * JSONTreeTransformator jtt = new JSONTreeTransformator();
 * List<Node> jsonTree = jtt.toTree(pointable, arg);
 */

public class JSONTreeTransformator {
    private final JSONTreeVisitor jTreeVisitor = new JSONTreeVisitor();
    private final JSONTreeSizeVisitor jTreeSizeVisitor = new JSONTreeSizeVisitor();
    private final JSONTreeLabelTypeIntersectionVisitor jIntersectionVisitor =
            new JSONTreeLabelTypeIntersectionVisitor();

    public JSONTreeTransformator() {
    }

    public List<Node> toTree(IVisitablePointable pointable, MutablePair<List<Node>, IntIntPair> arg)
            throws HyracksDataException {
        // Traverse the record and create an according JSON tree.
        pointable.accept(jTreeVisitor, arg);

        return arg.getLeft();
    }

    public int calculateTreeSize(IVisitablePointable pointable, MutableInt nodeCounter) throws HyracksDataException {
        pointable.accept(jTreeSizeVisitor, nodeCounter);

        return nodeCounter.intValue();
    }

    public MutablePair<HashMap<LabelTypeTuple, MutableInt>, MutableInt> countAndGenerateNodeBag(
            IVisitablePointable pointable, MutablePair<HashMap<LabelTypeTuple, MutableInt>, MutableInt> arg)
            throws HyracksDataException {
        pointable.accept(jIntersectionVisitor, arg);

        return arg;
    }

    public void reset() {
        // Free in-memory buffers for reuse.
        jTreeVisitor.reset();
        jIntersectionVisitor.reset();
    }
}
