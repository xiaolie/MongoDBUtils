/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.richeninfo.beanutils;

import java.beans.PropertyDescriptor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.beanutils.PropertyUtils;

/**
 *
 * @author xiaolie
 */
public class BeanUtilEx {
    
    static {
        ConvertUtils.register(new DateConverter(), java.util.Date.class);
    }

    private static boolean isPrimitive(Object value) {
        return value instanceof String || value instanceof Date || value.getClass().isPrimitive() || value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte || value instanceof Character || value instanceof Float || value instanceof Double || value instanceof Boolean || value instanceof BigInteger || value instanceof BigDecimal;
    }

    /**
     * 利用Introspector和PropertyDescriptor 将Bean --> Map
     * @param obj
     * @return
     */
    public static Map transBean2Map(Object obj) throws Exception {
        if (obj == null) {
            return null;
        }
        Map<String, Object> map = new HashMap<>();
        PropertyDescriptor[] propertyDescriptors = PropertyUtils.getPropertyDescriptors(obj);
        for (PropertyDescriptor property : propertyDescriptors) {
            String key = property.getName();
            if (!key.equals("class")) {
                try {
                    Object value = PropertyUtils.getProperty(obj, key);
                    if (value == null) {
                        map.put(key, value);
                    } else {
                        if (value instanceof List) {
                            List list = new ArrayList();
                            for (Object v : (List) value) {
                                list.add(transBean2Map(v));
                            }
                            map.put(key, list);
                        } else {
                            if (value instanceof Enum) {
                                value = value.toString();
                            }
                            if (isPrimitive(value)) {
                                map.put(key, value);
                            } else {
                                Map cmap = transBean2Map(value);
                                map.put(key, cmap);
                            }
                        }
                    }
                } catch (NoSuchMethodException e) {
                    System.out.println(e.toString());
                }
            }
        }
        return map;
    }
    
}
