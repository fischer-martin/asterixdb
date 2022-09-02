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

import org.apache.asterix.om.pointables.AFlatValuePointable;

/**
 * Use {@link JSONCostModel} to define a cost model for JSON similarity queries.
 * Example: CostModel cm = new CostModel(1, 1, 1);
 */

public class JSONCostModel {
    double ins;
    double del;
    double ren;

    public JSONCostModel(double ins, double del, double ren) {
        this.ins = ins;
        this.del = del;
        this.ren = ren;
    }

    public double ins(Node n) {
        return ins;
    }

    public double del(Node n) {
        return del;
    }

    public double ren(Node m, Node n) {
        if (m.getType() == n.getType()) {
            if (m.getLabel().getClass().getName().compareTo(n.getLabel().getClass().getName()) == 0) {
                if (m.getLabel() instanceof AFlatValuePointable) {
                    if (m.getLabel().equals(n.getLabel())) {
                        return 0; // 0 cost if the node type, data type, and values are identical.
                    }
                    return ren; // Rename cost if values are different.
                } else {
                    return 0; // Objects, arrays, and multisets always match.
                }
            } else { // Comment else to disable renames of different literal types.
                return ren; // Rename cost if data types are different.
            }
        }
        return Double.POSITIVE_INFINITY; // Different node types.
    }
}