package org.apache.asterix.external.cartilage.base;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

public class ObjectInputStreamWithLoader extends ObjectInputStream {
    private ClassLoader classLoader;
    public ObjectInputStreamWithLoader(InputStream in, ClassLoader classLoader) throws IOException {
        super(in);
        this.classLoader = classLoader;
    }
    protected Class<?> resolveClass(ObjectStreamClass desc)
            throws IOException, ClassNotFoundException
    {
        String name = desc.getName();
        return Class.forName(name, false, this.classLoader);
    }
}
