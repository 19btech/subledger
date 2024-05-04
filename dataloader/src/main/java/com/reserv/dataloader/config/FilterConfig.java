package com.reserv.dataloader.config;

import com.reserv.dataloader.component.TenantContextHolder;
import com.reserv.dataloader.filter.TenantFilter;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;

@Configuration
public class FilterConfig {

    @Bean
    public FilterRegistrationBean<TenantFilter> tenantFilterRegistration(TenantContextHolder tenantContextHolder) {
        FilterRegistrationBean<TenantFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(new TenantFilter(tenantContextHolder));
        registrationBean.setOrder(1); // Set the order to execute this filter first
        registrationBean.setUrlPatterns(Collections.singletonList("/*")); // Intercept all requests
        return registrationBean;
    }
}
