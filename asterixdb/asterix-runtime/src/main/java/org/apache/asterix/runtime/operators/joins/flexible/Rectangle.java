package org.apache.asterix.runtime.operators.joins.flexible;

import java.io.Serializable;

public class Rectangle implements Serializable {
    public double x1, x2, y1, y2 = 0.0;

    public Rectangle(double x1, double x2, double y1, double y2) {
        this.x1 = x1;
        this.x2 = x2;
        this.y1 = y1;
        this.y2 = y2;
    }

    public Rectangle() {
    }

    ;
}
