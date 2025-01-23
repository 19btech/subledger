package com.fyntrac.common.service;

import com.fyntrac.common.entity.Tenant;
import com.fyntrac.common.repository.TenantRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TenantService {

    private final TenantRepo tenantRepository;

    @Autowired
    public TenantService(TenantRepo tenantRepository) {
        this.tenantRepository = tenantRepository;
    }

    public List<Tenant> getAllTenants() {
        return tenantRepository.findAll();
    }
}