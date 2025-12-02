package com.fyntrac.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
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
                resultMap.put(field.getName().toUpperCase(), field.get(object));
            } catch (IllegalAccessException e) {
                // Handle the exception (e.g., log it)
                log.error(StringUtil.getStackTrace(e));
                throw  e;
            }
        }

        return resultMap;
    }

    public static <T> Map<String, Object> convertRecordToFlatMap(T obj) {
        Map<String, Object> flatMap = new HashMap<>();
        if (obj != null) {
            flattenRecordRecursive("", obj, flatMap);
        }
        return flatMap;
    }

    private static void flattenRecordRecursive(String parentKey, Object obj, Map<String, Object> flatMap) {
        if (obj == null) return;

        Class<?> clazz = obj.getClass();

        // Handle primitives, wrappers, dates directly
        if (isPrimitiveOrWrapper(obj) || obj instanceof Date) {
            flatMap.put(parentKey.toUpperCase(), obj);
            return;
        }

        // Handle Map
        if (obj instanceof Map<?, ?> mapObj) {
            flattenMap(parentKey, mapObj, flatMap);
            return;
        }

        // Handle List
        if (obj instanceof List<?> listObj) {
            for (int i = 0; i < listObj.size(); i++) {
                flattenObjectRecursive(parentKey + "[" + i + "]", listObj.get(i), flatMap);
            }
            return;
        }

        // Handle Records
        if (clazz.isRecord()) {
            for (RecordComponent component : clazz.getRecordComponents()) {
                try {
                    Method accessor = component.getAccessor();
                    Object value = accessor.invoke(obj);
                    if (value == null) continue;

                    String key = parentKey.isEmpty() ? component.getName() : parentKey + "." + component.getName();
                    flattenRecordRecursive(key, value, flatMap);

                } catch (Exception e) {
                    System.err.println("Failed to access record component: " + component.getName());
                }
            }
            return;
        }

        // Fallback for POJO
        for (Field field : clazz.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) continue;

            field.setAccessible(true);
            try {
                Object value = field.get(obj);
                if (value == null) continue;

                String key = parentKey.isEmpty() ? field.getName() : parentKey + "." + field.getName();
                flattenObjectRecursive(key, value, flatMap);

            } catch (IllegalAccessException e) {
                System.err.println("Skipping inaccessible field: " + field.getName());
            }
        }
    }

    public static <T> Map<String, Object> convertToFlatMap(T obj) {
        Map<String, Object> flatMap = new HashMap<>();
        if (obj != null) {
            flattenObjectRecursive("", obj, flatMap);
        }
        return flatMap;
    }

    private static void flattenObjectRecursive(String parentKey, Object obj, Map<String, Object> flatMap) {
        if (obj == null) {
            return;
        }

        // Handle Date objects explicitly
        if (obj instanceof Date) {
            flatMap.put(parentKey.toUpperCase(), obj);
            return;
        }

        Class<?> clazz = obj.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            // Skip static, final, or synthetic fields
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers()) ||
                    java.lang.reflect.Modifier.isFinal(field.getModifiers()) ||
                    field.isSynthetic()) {
                continue;
            }

            field.setAccessible(true);
            try {
                String key = parentKey.isEmpty() ? field.getName() : parentKey + "." + field.getName();
                Object value = field.get(obj);

                if (value == null) {
                    continue;
                } else if (isPrimitiveOrWrapper(value) || value instanceof Date) {
                    if (value instanceof String str) {
                        str = str.trim();
                        if (str.matches("-?\\d+")) {
                            value = Integer.parseInt(str);
                        } else if (str.matches("-?\\d*\\.\\d+")) {
                            value = Double.parseDouble(str);
                        } else {
                            value = str; // keep as String if not a number
                        }
                    }

                    flatMap.put(key.toUpperCase(), value);
                } else if (value instanceof Map) {
                    flattenMap(key, (Map<?, ?>) value, flatMap);
                } else if (value instanceof List) {
                    List<?> list = (List<?>) value;
                    for (int i = 0; i < list.size(); i++) {
                        flattenObjectRecursive(key + "[" + i + "]", list.get(i), flatMap);
                    }
                } else {
                    flattenObjectRecursive(key, value, flatMap);
                }
            } catch (IllegalAccessException e) {
                System.err.println("Skipping inaccessible field: " + field.getName());
            }
        }
    }

    private static boolean isPrimitiveOrWrapper(Object obj) {
        Class<?> clazz = obj.getClass();
        return clazz.isPrimitive()
                || clazz == Boolean.class || clazz == Byte.class || clazz == Character.class
                || clazz == Short.class || clazz == Integer.class || clazz == Long.class
                || clazz == Float.class || clazz == Double.class || clazz == String.class
                || clazz.isEnum(); // <- add this
    }


    private static void flattenMap(String parentKey, Map<?, ?> map, Map<String, Object> flatMap) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String key = parentKey + "." + entry.getKey().toString();
            Object value = entry.getValue();

            if (value == null) {
                continue;
            } else if (isPrimitiveOrWrapper(value) || value instanceof Date || value instanceof Enum) {
                flatMap.put(key.toUpperCase(), value);
            } else if (value instanceof Map) {
                flattenMap(key, (Map<?, ?>) value, flatMap);
            } else if (value instanceof List) {
                List<?> list = (List<?>) value;
                for (int i = 0; i < list.size(); i++) {
                    flattenObjectRecursive(key + "[" + i + "]", list.get(i), flatMap);
                }
            } else {
                flattenObjectRecursive(key, value, flatMap);
            }
        }
    }

}