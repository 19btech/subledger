package com.fyntrac.common.config;

import org.springframework.stereotype.Component;

@Component
public class TenantContextHolder {

    private static final ThreadLocal<String> currentTenant = new ThreadLocal<>();

    public static String getTenant() {
        return currentTenant.get();
    }

    public static void setTenant(String tenantId) {
        currentTenant.set(tenantId);
    }

    public static void clear() {
        currentTenant.remove();
    }


}
