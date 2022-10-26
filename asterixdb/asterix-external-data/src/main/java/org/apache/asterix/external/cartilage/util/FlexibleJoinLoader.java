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
package org.apache.asterix.external.cartilage.util;

import static org.apache.asterix.external.cartilage.util.ParameterTypeResolver.getTypedObjectsParametersArray;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.external.cartilage.base.FlexibleJoin;
import org.apache.asterix.external.library.ExternalLibraryManager;
import org.apache.asterix.external.library.JavaLibrary;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.functions.ExternalFJFunctionInfo;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class FlexibleJoinLoader {
    public static ClassLoader getFlexibleJoinClassLoader(ExternalFJFunctionInfo finfo, IEvaluatorContext context)
            throws HyracksDataException {
        DataverseName libraryDataverseName = finfo.getLibraryDataverseName();
        String libraryName = finfo.getLibraryName();
        ExternalLibraryManager libraryManager =
                (ExternalLibraryManager) ((INcApplicationContext) context.getServiceContext().getApplicationContext())
                        .getLibraryManager();
        JavaLibrary library = (JavaLibrary) libraryManager.getLibrary(libraryDataverseName, libraryName);
        return library.getClassLoader();
    }

    public static FlexibleJoin<?, ?> getFlexibleJoin(ExternalFJFunctionInfo finfo, ClassLoader classLoader)
            throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException {
        String classname = finfo.getExternalIdentifier().get(0);
        Class<?> flexibleJoinClass = Class.forName(classname, true, classLoader);

        Constructor<?> flexibleJoinConstructor = flexibleJoinClass.getConstructors()[0];
        List<IAObject> parameters = finfo.getParameters();
        return (FlexibleJoin<?, ?>) flexibleJoinConstructor.newInstance(getTypedObjectsParametersArray(parameters));

    }
}
