package com.fyntrac.common.config;

import org.springframework.stereotype.Component;

@Component
public class TenantContextHolder {

    private static final ThreadLocal<String> currentTenant = new ThreadLocal<>();

    public String getTenant() {
        return currentTenant.get();
    }

    public void setTenant(String tenantId) {
        currentTenant.set(tenantId);
    }

    public void clear() {
        currentTenant.remove();
    }
}
