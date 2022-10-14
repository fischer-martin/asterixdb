package org.apache.asterix.runtime.evaluators.common;

import java.util.Objects;

import org.apache.asterix.om.pointables.base.IVisitablePointable;

public class LabelTypeTuple {

    private IVisitablePointable label; // null if type is object, array, or multiset
    private int type; // 1: literal, 2: key, 3: object, 4: array, 5: multiset

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LabelTypeTuple that = (LabelTypeTuple) o;

        return type == that.type && Objects.equals(label, that.label);
    }

    private int hashArraySlice(byte[] arr, int start, int end) {
        int result = 0;

        for (int i = start; i < end; ++i) {
            result = 31 * result + arr[i];
        }

        return result;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(type);

        if (type == 1 || type == 2) {
            result = result * 31 + hashArraySlice(label.getByteArray(), label.getStartOffset(),
                    label.getStartOffset() + label.getLength());
        }

        return result;
    }

    public IVisitablePointable getLabel() {
        return label;
    }

    public int getType() {
        return type;
    }

    public void setLabel(IVisitablePointable label) {
        this.label = label;
    }

    public void setType(int type) {
        this.type = type;
    }
}
