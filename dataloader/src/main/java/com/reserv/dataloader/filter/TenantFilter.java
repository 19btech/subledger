package com.reserv.dataloader.filter;

import  com.fyntrac.common.config.TenantContextHolder;
import com.reserv.dataloader.exception.TenantNotFoundException;
import com.fyntrac.common.service.DataService;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.xml.crypto.Data;
import java.io.IOException;

@Component
public class TenantFilter implements Filter {

    private final TenantContextHolder tenantContextHolder;
    private final DataService dataService;
    @Autowired
    public TenantFilter(TenantContextHolder tenantContextHolder, DataService dataService) {
        this.tenantContextHolder = tenantContextHolder;
        this.dataService = dataService;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException{
        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
        String tenantId = httpRequest.getHeader("X-Tenant".toLowerCase());
        tenantContextHolder.setTenant(tenantId);
        this.dataService.setTenantId(tenantId);
        filterChain.doFilter(servletRequest, servletResponse);
        // tenantContextHolder.clear(); // Clear tenant after request processing
    }
}