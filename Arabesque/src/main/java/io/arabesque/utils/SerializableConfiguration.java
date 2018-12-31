package io.arabesque.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Created by ehussein on 10/18/17.
 */
public class SerializableConfiguration implements Serializable {
    public transient Configuration value;
    private static final Logger LOG = Logger.getLogger(SerializableConfiguration.class);

    public SerializableConfiguration(Configuration _value) {
        this.value = _value;
    }

    private void writeObject(ObjectOutputStream out) {
        try {
            out.defaultWriteObject();
            value.write(out);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void readObject(ObjectInputStream in) {
        try {
            value = new Configuration(false);
            value.readFields(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
