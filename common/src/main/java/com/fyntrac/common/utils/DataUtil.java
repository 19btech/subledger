package com.fyntrac.common.utils;

import com.fyntrac.common.dto.record.RecordFactory;
import com.fyntrac.common.dto.record.Records;

import java.util.*;
import java.util.stream.Collectors;

public class DataUtil {

    public static List<Records.DocumentAttribute> convert(
            List<Map<String, Object>> list,
            Set<String> exclusionKeys) {

        List<Records.DocumentAttribute> result = new ArrayList<>();
        Set<String> seenKeys = new HashSet<>();

        // Normalize exclusionKeys to lowercase for case-insensitive matching
        Set<String> normalizedExclusionKeys = null;
        if (exclusionKeys != null) {
            normalizedExclusionKeys = exclusionKeys.stream()
                    .filter(Objects::nonNull)
                    .map(String::toLowerCase)
                    .collect(Collectors.toSet());
        }

        for (Map<String, Object> map : list) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String originalKey = entry.getKey();
                String normalizedKey = originalKey.toLowerCase();

                // Convert ID (case-insensitive) into "id"
                String attributeName = originalKey.equalsIgnoreCase("ID") ? "id" : originalKey;

                // Exclude if in exclusionKeys (case-insensitive)
                if ((normalizedExclusionKeys == null || !normalizedExclusionKeys.contains(normalizedKey))
                        && seenKeys.add(attributeName.toLowerCase())) {

                    String dataType = entry.getValue() == null
                            ? "Object"
                            : entry.getValue().getClass().getSimpleName();

                    result.add(RecordFactory.createDocumentAttribute(attributeName, attributeName, dataType));
                }
            }
        }

        return result;
    }

    public static List<Map<String, Object>> normalizeIdKeys(
            List<Map<String, Object>> inputList,
            Set<String> exclusionKeys) {

        if (inputList == null) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> normalizedList = new ArrayList<>();
        for (Map<String, Object> originalMap : inputList) {
            Map<String, Object> newMap = new HashMap<>();
            for (Map.Entry<String, Object> entry : originalMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                // Skip if excluded
                if (exclusionKeys != null && exclusionKeys.contains(key)) {
                    continue;
                }

                if ("ID".equalsIgnoreCase(key)) {
                    newMap.put("id", value); // normalize to lowercase "id"
                } else {
                    newMap.put(key, value);
                }
            }
            normalizedList.add(newMap);
        }
        return normalizedList;
    }

    public static List<Map<String, Object>> ensureIds(List<Map<String, Object>> list) {
        int counter = 1;
        for (Map<String, Object> map : list) {
            if (!map.containsKey("id")) {
                map.put("id", counter++);
            }
        }
        return list;
    }

    public static List<Map<String, Object>> reorderKeys(
            List<Map<String, Object>> list, List<String> priorityKeys) {

        List<Map<String, Object>> result = new ArrayList<>();

        for (Map<String, Object> original : list) {
            // LinkedHashMap preserves insertion order
            Map<String, Object> reordered = new LinkedHashMap<>();

            // First, put all priority keys (case-insensitive match)
            for (String priorityKey : priorityKeys) {
                for (String originalKey : original.keySet()) {
                    if (originalKey.equalsIgnoreCase(priorityKey)) {
                        reordered.put(originalKey, original.get(originalKey));
                        break; // stop at first match
                    }
                }
            }

            // Then, put the rest in the same order as original
            for (Map.Entry<String, Object> entry : original.entrySet()) {
                boolean alreadyAdded = reordered.keySet().stream()
                        .anyMatch(k -> k.equalsIgnoreCase(entry.getKey()));
                if (!alreadyAdded) {
                    reordered.put(entry.getKey(), entry.getValue());
                }
            }

            result.add(reordered);
        }

        return result;
    }

}
