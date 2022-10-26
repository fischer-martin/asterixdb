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
package org.apache.hyracks.algebricks.core.algebra.functions;

import java.util.List;

import org.apache.commons.lang3.mutable.Mutable;
import org.apache.hyracks.algebricks.core.algebra.base.ILogicalExpression;

public class FlexibleJoinWrapperFunctionIdentifier extends FunctionIdentifier {
    private static final long serialVersionUID = 1L;
    public static final int VARARGS = -1;

    private String libraryName = "";
    private List<Mutable<ILogicalExpression>> parameters;

    public FlexibleJoinWrapperFunctionIdentifier(String namespace, String name, int arity) {
        super(namespace, name, arity);
    }

    public String getLibraryName() {
        return libraryName;
    }

    public void setLibraryName(String libraryName) {
        this.libraryName = libraryName;
    }

    public void setParameters(List<Mutable<ILogicalExpression>> parameters) {
        this.parameters = parameters;
    }

    public List<Mutable<ILogicalExpression>> getParameters() {
        return this.parameters;
    }

}
