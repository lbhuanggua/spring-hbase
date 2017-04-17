package com.lb.spring.hbase.admin;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    private Utils(){}

    public static long toLong(Object obj){
        long l = 0;
        if(obj != null){
            if(obj instanceof Number){
                l = ((Number)obj).longValue();
            } else{
                LOG.warn("Could not coerce {} to Long", obj.getClass().getName());
            }
        }
        return l;
    }

    public static byte[] toBytes(Object obj) {
        if(obj == null) {
          return null;
        } else if(obj instanceof String){
            return ((String)obj).getBytes();
        } else if (obj instanceof Integer){
            return Bytes.toBytes((Integer) obj);
        } else if (obj instanceof Long){
            return Bytes.toBytes((Long)obj);
        } else if (obj instanceof Short){
            return Bytes.toBytes((Short)obj);
        } else if (obj instanceof Float){
            return Bytes.toBytes((Float)obj);
        } else if (obj instanceof Double){
            return Bytes.toBytes((Double)obj);
        } else if (obj instanceof Boolean){
            return Bytes.toBytes((Boolean)obj);
        } else if (obj instanceof BigDecimal){
            return Bytes.toBytes((BigDecimal)obj);
        } else {
            LOG.error("Can't convert class to byte array: " + obj.getClass().getName());
            return new byte[0];
        }
    }
}
