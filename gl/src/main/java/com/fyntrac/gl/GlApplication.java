package com.fyntrac.gl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"com.fyntrac.common", "com.fyntrac.gl"})
public class GlApplication {

	public static void main(String[] args) {
		SpringApplication.run(GlApplication.class, args);
	}

}
