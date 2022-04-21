package org.apache.asterix.external.cartilage.util;

import org.apache.asterix.om.base.*;

import java.util.List;

public class ParameterTypeResolver {
    public static Object[] getTypedObjectsParametersArray(List<IAObject> parameters) {
        Object[] parametersArray = new Object[parameters.size()];
        for (int i = 0; i < parameters.size(); i ++) {
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
}
