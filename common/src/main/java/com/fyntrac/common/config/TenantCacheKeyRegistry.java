package com.fyntrac.common.config;

import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class TenantCacheKeyRegistry {

    private static final ConcurrentMap<String, Set<String>> tenantKeysMap = new ConcurrentHashMap<>();

    // Register a cache key under a tenant
    public static void registerKey(String tenantId, String key) {
        tenantKeysMap
                .computeIfAbsent(tenantId, id -> ConcurrentHashMap.newKeySet())
                .add(key);
    }

    // Get all keys for a tenant
    public static Set<String> getKeys(String tenantId) {
        return tenantKeysMap.getOrDefault(tenantId, Set.of());
    }

    // Remove and return all keys for a tenant
    public static Set<String> removeAllKeys(String tenantId) {
        return tenantKeysMap.remove(tenantId);
    }

    // Check if registry has keys for tenant
    public static boolean hasKeys(String tenantId) {
        return tenantKeysMap.containsKey(tenantId);
    }

    // Clear all tenant mappings
    public static void clearAll() {
        tenantKeysMap.clear();
    }
}
