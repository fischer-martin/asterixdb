package org.apache.asterix.external.cartilage.util;

import org.apache.asterix.dataflow.data.nontagged.serde.ABooleanSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADateTimeSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADayTimeDurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADoubleSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.ADurationSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AFloatSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AGeometrySerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt16SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt32SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt64SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AInt8SerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AIntervalSerializerDeserializer;
import org.apache.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import org.apache.asterix.om.base.*;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import java.io.DataInputStream;
import java.util.List;

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
                returnObject = ADayTimeDurationSerializerDeserializer.INSTANCE.deserialize(dataInputStream).getMilliseconds();
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
            case INTERVAL:
                returnObject = AIntervalSerializerDeserializer.INSTANCE.deserialize(dataInputStream).toString();
                break;
            case STRING:
                returnObject = AStringSerializerDeserializer.INSTANCE.deserialize(dataInputStream).getStringValue();
                break;
        }
        return returnObject;
    }
}
