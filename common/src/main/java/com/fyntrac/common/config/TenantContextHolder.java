package com.fyntrac.common.config;

import org.springframework.stereotype.Component;

import java.util.function.Supplier;

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

    /**
     * Default method to run a task with the tenant context.
     *
     * @param tenant The tenant ID to set in the context.
     * @param task   The task to execute.
     */
    public static void runWithTenant(String tenant, Runnable task) {
        String previousTenant = currentTenant.get(); // Save the previous tenant context
        try {
            currentTenant.set(tenant); // Set the new tenant context
            task.run(); // Execute the task
        } finally {
            currentTenant.set(previousTenant); // Restore the previous tenant context
        }
    }

    /**
     * Default method to run a task with the tenant context and return a result.
     *
     * @param tenant The tenant ID to set in the context.
     * @param task   The task to execute.
     * @return The result of the task.
     */
    public static <T> T runWithTenant(String tenant, Supplier<T> task) {
        String previousTenant = currentTenant.get(); // Save the previous tenant context
        try {
            currentTenant.set(tenant); // Set the new tenant context
            return task.get(); // Execute the task and return the result
        } finally {
            currentTenant.set(previousTenant); // Restore the previous tenant context
        }
    }
}
