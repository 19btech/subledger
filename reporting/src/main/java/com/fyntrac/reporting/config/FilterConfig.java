package com.fyntrac.reporting.config;

import com.fyntrac.common.config.TenantDatasourceConfig;
import com.fyntrac.common.service.DataService;
import com.fyntrac.reporting.filter.TenantFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;

@Configuration
public class FilterConfig {

    @Bean
    public FilterRegistrationBean<TenantFilter> tenantFilterRegistration( DataService dataService,
                                                                          TenantDatasourceConfig tenantDatasourceConfig) {
        FilterRegistrationBean<TenantFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(new TenantFilter( dataService, tenantDatasourceConfig));
        registrationBean.setOrder(1); // Set the order to execute this filter first
        registrationBean.setUrlPatterns(Collections.singletonList("/*")); // Intercept all requests
        return registrationBean;
    }
}

