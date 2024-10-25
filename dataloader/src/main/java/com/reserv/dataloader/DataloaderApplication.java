package com.reserv.dataloader;

import com.reserv.dataloader.initializer.DatabaseInitializer;
import javax.sql.DataSource;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication(scanBasePackages = {"com.fyntrac.common", "com.reserv.dataloader"})

public class DataloaderApplication {

	@Bean
	public CommandLineRunner databaseInitializer(DataSource dataSource) {
		return new DatabaseInitializer(dataSource);
	}

	public static void main(String[] args) {
		SpringApplication.run(DataloaderApplication.class, args);
	}
}
