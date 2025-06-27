package com.reserv.dataloader;

import com.reserv.dataloader.initializer.DatabaseInitializer;
import javax.sql.DataSource;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Date;

@SpringBootApplication(scanBasePackages = {"com.fyntrac.common", "com.reserv.dataloader"})

public class DataloaderApplication {

	@Bean
	public CommandLineRunner databaseInitializer(DataSource dataSource) {
		return new DatabaseInitializer(dataSource);
	}

	@Bean
	public MongoCustomConversions customConversions() {
		return new MongoCustomConversions(Arrays.asList(
				new DateToInstantConverter(),
				new InstantToDateConverter()
		));
	}

	static class DateToInstantConverter implements Converter<Date, Instant> {
		@Override
		public Instant convert(Date source) {
			return source.toInstant();
		}
	}

	static class InstantToDateConverter implements Converter<Instant, Date> {
		@Override
		public Date convert(Instant source) {
			return Date.from(source.atZone(ZoneOffset.UTC).toInstant());
		}
	}


	public static void main(String[] args) {
		SpringApplication.run(DataloaderApplication.class, args);
	}
}
