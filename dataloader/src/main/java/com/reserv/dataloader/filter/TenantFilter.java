package com.reserv.dataloader.filter;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.config.TenantDatasourceConfig;
import com.fyntrac.common.service.DataService;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class TenantFilter implements Filter {

    private final DataService dataService;
    private final TenantDatasourceConfig tenantDatasourceConfig;
    @Autowired
    public TenantFilter(DataService dataService,
                        TenantDatasourceConfig tenantDatasourceConfig) {
        this.dataService = dataService;
        this.tenantDatasourceConfig = tenantDatasourceConfig;
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException{
        HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
        String tenantId = httpRequest.getHeader("X-Tenant".toLowerCase());
        this.tenantDatasourceConfig.configureTenantDatabases(tenantId);
        if(tenantId != null) {
            TenantContextHolder.setTenant(tenantId);
        }
        this.dataService.setTenantId(tenantId);
        filterChain.doFilter(servletRequest, servletResponse);
        // tenantContextHolder.clear(); // Clear tenant after request processing
    }
}