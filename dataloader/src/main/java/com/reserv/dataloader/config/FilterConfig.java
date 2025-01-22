package  com.reserv.dataloader.config;

import com.reserv.dataloader.filter.TenantFilter;
import com.fyntrac.common.service.DataService;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.fyntrac.common.config.TenantContextHolder;

import java.util.Collections;

@Configuration
public class FilterConfig {

    @Bean
    public FilterRegistrationBean<TenantFilter> tenantFilterRegistration( DataService dataService) {
        FilterRegistrationBean<TenantFilter> registrationBean = new FilterRegistrationBean<>();
        registrationBean.setFilter(new TenantFilter( dataService));
        registrationBean.setOrder(1); // Set the order to execute this filter first
        registrationBean.setUrlPatterns(Collections.singletonList("/*")); // Intercept all requests
        return registrationBean;
    }
}
