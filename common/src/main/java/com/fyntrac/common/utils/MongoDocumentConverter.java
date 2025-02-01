package com.fyntrac.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class MongoDocumentConverter {

    public static <T> Map<String, Object> convertToMap(T object) throws Throwable{
        Map<String, Object> resultMap = new HashMap<>();

        if (object == null) {
            return resultMap; // Return an empty map if the object is null
        }

        // Get all fields of the object's class
        Field[] fields = object.getClass().getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true); // Allow access to private fields
            try {
                // Put the field name and its value into the map
                resultMap.put(field.getName(), field.get(object));
            } catch (IllegalAccessException e) {
                // Handle the exception (e.g., log it)
                log.error(StringUtil.getStackTrace(e));
                throw  e;
            }
        }

        return resultMap;
    }
}