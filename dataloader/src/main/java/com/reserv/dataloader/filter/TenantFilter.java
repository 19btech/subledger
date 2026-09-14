package com.reserv.dataloader.filter;

import com.fyntrac.common.config.TenantContextHolder;
import com.fyntrac.common.config.TenantDatasourceConfig;
import com.fyntrac.common.service.DataService;
import jakarta.servlet.*;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

// Not a @Component: this filter is registered explicitly via the FilterRegistrationBean
// in FilterConfig so its order can be controlled. Adding @Component here as well would
// make Spring Boot auto-register a *second*, independent instance, causing every request
// to run tenant-context setup/teardown twice.
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
        try {
            HttpServletRequest httpRequest = (HttpServletRequest) servletRequest;
            String tenantId = httpRequest.getHeader("X-Tenant".toLowerCase());
            this.tenantDatasourceConfig.configureTenantDatabases(tenantId);
            if (tenantId != null) {
                TenantContextHolder.setTenant(tenantId);
            }
            this.dataService.setTenantId(tenantId);
            filterChain.doFilter(servletRequest, servletResponse);
        }finally {
            TenantContextHolder.clear(); // Clear tenant after request processing
        }
        //
    }
}