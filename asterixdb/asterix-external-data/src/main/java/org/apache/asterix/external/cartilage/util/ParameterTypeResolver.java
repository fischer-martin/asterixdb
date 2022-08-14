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

import java.io.DataInputStream;
import java.util.List;

import org.apache.asterix.dataflow.data.nontagged.serde.*;
import org.apache.asterix.external.cartilage.base.types.Interval;
import org.apache.asterix.external.cartilage.base.types.Rectangle;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ADate;
import org.apache.asterix.om.base.ADateTime;
import org.apache.asterix.om.base.ADayTimeDuration;
import org.apache.asterix.om.base.ADouble;
import org.apache.asterix.om.base.ADuration;
import org.apache.asterix.om.base.AFloat;
import org.apache.asterix.om.base.AGeometry;
import org.apache.asterix.om.base.AInt16;
import org.apache.asterix.om.base.AInt32;
import org.apache.asterix.om.base.AInt64;
import org.apache.asterix.om.base.AInt8;
import org.apache.asterix.om.base.AInterval;
import org.apache.asterix.om.base.ARectangle;
import org.apache.asterix.om.base.AString;
import org.apache.asterix.om.base.IAObject;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ParameterTypeResolver {
    public static Object[] getTypedObjectsParametersArray(List<IAObject> parameters) {
        Object[] parametersArray = new Object[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
            IAObject p = parameters.get(i);
            switch (p.getType().getTypeTag()) {
                case BOOLEAN:
                    parametersArray[i] = ((ABoolean) p).getBoolean();
                    break;
                case DATE:
                    parametersArray[i] = ((ADate) p).getChrononTimeInDays();
                    break;
                case DATETIME:
                    parametersArray[i] = ((ADateTime) p).getChrononTime();
                    break;
                case DAYTIMEDURATION:
                    parametersArray[i] = ((ADayTimeDuration) p).getMilliseconds();
                    break;
                case DOUBLE:
                    parametersArray[i] = ((ADouble) p).getDoubleValue();
                    break;
                case DURATION:
                    parametersArray[i] = ((ADuration) p).getMilliseconds();
                    break;
                case FLOAT:
                    parametersArray[i] = ((AFloat) p).getFloatValue();
                    break;
                case GEOMETRY:
                    parametersArray[i] = ((AGeometry) p).getGeometry();
                    break;
                case TINYINT:
                    parametersArray[i] = ((AInt8) p).getByteValue();
                    break;
                case SMALLINT:
                    parametersArray[i] = ((AInt16) p).getShortValue();
                    break;
                case INTEGER:
                    parametersArray[i] = ((AInt32) p).getIntegerValue();
                    break;
                case BIGINT:
                    parametersArray[i] = ((AInt64) p).getLongValue();
                    break;
                case INTERVAL:
                    parametersArray[i] = ((AInterval) p).toString();
                    break;
                case STRING:
                    parametersArray[i] = ((AString) p).getStringValue();
                    break;
                case OBJECT:
                    parametersArray[i] = p;
                    break;
            }
        }
        return parametersArray;
    }

    public static Object getKeyObject(DataInputStream dataInputStream, ATypeTag dataType) throws HyracksDataException {
        Object returnObject = null;
        switch (dataType) {
            case BOOLEAN:
                returnObject = ABooleanSerializerDeserializer.INSTANCE.deserialize(dataInputStream).getBoolean();
                break;
            case DATE:
                returnObject = ADateSerializerDeserializer.INSTANCE.deserialize(dataInputStream).getChrononTimeInDays();
                break;
            case DATETIME:
                returnObject = ADateTimeSerializerDeserializer.INSTANCE.deserialize(dataInputStream).getChrononTime();
                break;
            case DAYTIMEDURATION:
                returnObject =
                        ADayTimeDurationSerializerDeserializer.INSTANCE.deserialize(dataInputStream).getMilliseconds();
                break;
            case DOUBLE:
                returnObject = ADoubleSerializerDeserializer.INSTANCE.deserialize(dataInputStream).getDoubleValue();
                break;
            case DURATION:
                returnObject = ADurationSerializerDeserializer.INSTANCE.deserialize(dataInputStream).getMilliseconds();
                break;
            case FLOAT:
                returnObject = AFloatSerializerDeserializer.INSTANCE.deserialize(dataInputStream).getFloatValue();
                break;
            case GEOMETRY:
                returnObject = AGeometrySerializerDeserializer.INSTANCE.deserialize(dataInputStream).getGeometry();
                break;
            case TINYINT:
                returnObject = AInt8SerializerDeserializer.INSTANCE.deserialize(dataInputStream).getByteValue();
                break;
            case SMALLINT:
                returnObject = AInt16SerializerDeserializer.INSTANCE.deserialize(dataInputStream).getShortValue();
                break;
            case INTEGER:
                returnObject = AInt32SerializerDeserializer.INSTANCE.deserialize(dataInputStream).getIntegerValue();
                break;
            case BIGINT:
                returnObject = AInt64SerializerDeserializer.INSTANCE.deserialize(dataInputStream).getLongValue();
                break;
            case INTERVAL: {
                //returnObject = AIntervalSerializerDeserializer.INSTANCE.deserialize(dataInputStream).toString();
                AInterval interval = AIntervalSerializerDeserializer.INSTANCE.deserialize(dataInputStream);
                long start0 = interval.getIntervalStart();
                long end0 = interval.getIntervalEnd();
                returnObject = new Interval(start0, end0);
                break;
            }
            case STRING:
                returnObject = AStringSerializerDeserializer.INSTANCE.deserialize(dataInputStream).getStringValue();
                break;
            case RECTANGLE: {
                ARectangle rectangle = ARectangleSerializerDeserializer.INSTANCE.deserialize(dataInputStream);
                double minX1 = rectangle.getP1().getX();
                double minY1 = rectangle.getP1().getY();
                double maxX1 = rectangle.getP2().getX();
                double maxY1 = rectangle.getP2().getY();
                returnObject = new Rectangle(minX1, maxX1, minY1, maxY1);
                break;
            }
            case ANY:
                returnObject = dataInputStream;
        }
        return returnObject;
    }
}
