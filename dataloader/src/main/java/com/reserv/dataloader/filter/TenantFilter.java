package com.reserv.dataloader.filter;

import com.reserv.dataloader.component.TenantContextHolder;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class TenantFilter implements Filter {

    private final TenantContextHolder tenantContextHolder;

    @Autowired
    public TenantFilter(TenantContextHolder tenantContextHolder) {
        this.tenantContextHolder = tenantContextHolder;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
        String tenantId = httpRequest.getHeader("X-Tenant".toLowerCase());
        tenantContextHolder.setTenant(tenantId);
        filterChain.doFilter(servletRequest, servletResponse);
        // tenantContextHolder.clear(); // Clear tenant after request processing
    }
}