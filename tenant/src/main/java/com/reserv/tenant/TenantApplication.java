package com.reserv.tenant;

import com.reserv.tenant.repo.MerchantRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class TenantApplication {

	@Autowired
	MerchantRepo merchantRepo;

	public static void main(String[] args) {
		SpringApplication.run(TenantApplication.class, args);
	}
}
