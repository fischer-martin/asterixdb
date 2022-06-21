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
package org.apache.hyracks.dataflow.std.structures;

import org.apache.hyracks.data.std.accessors.MurmurHash3BinaryHash;

public final class KeyPair implements IResetable<KeyPair> {
    public static final int INVALID_ID = -1;
    private int buildKey;
    private int probeKey;

    public KeyPair() {
        this(INVALID_ID, INVALID_ID);
    }

    public KeyPair(int buildKey, int probeKey) {
        reset(buildKey, probeKey);
    }

    public int getBuildKey() {
        return buildKey;
    }

    public int getProbeKey() {
        return probeKey;
    }

    @Override
    public void reset(KeyPair other) {
        reset(other.buildKey, other.probeKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o == null || getClass() != o.getClass()) {
            return false;
        } else {
            final KeyPair that = (KeyPair) o;
            return buildKey == that.buildKey && probeKey == that.probeKey;
        }
    }

    public void reset(int buildKey, int probeKey) {
        this.buildKey = buildKey;
        this.probeKey = probeKey;
    }

    @Override
    public String toString() {
        return "KeyPair(" + buildKey + ", " + probeKey + ")";
    }

}
