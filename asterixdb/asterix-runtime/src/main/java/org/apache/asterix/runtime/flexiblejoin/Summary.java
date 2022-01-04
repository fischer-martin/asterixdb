package org.apache.asterix.runtime.flexiblejoin;

public interface Summary<T> {
    void add(T k);

    void add(Summary<T> s);
}
