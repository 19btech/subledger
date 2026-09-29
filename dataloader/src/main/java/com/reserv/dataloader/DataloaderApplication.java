package com.reserv.dataloader;

import com.reserv.dataloader.initializer.DatabaseInitializer;
import javax.sql.DataSource;
import org.apache.poi.util.IOUtils;
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
@org.springframework.data.jpa.repository.config.EnableJpaRepositories(basePackages = {"com.fyntrac.common.repository", "com.reserv.dataloader.repository"})
@org.springframework.boot.autoconfigure.domain.EntityScan(basePackages = {"com.fyntrac.common.entity", "com.reserv.dataloader.entity"})
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


	// POI refuses to read any single part of an .xlsx larger than 100MB unpacked. Client activity files
	// exceed that (Hearst DSH_SOD_20260731: 10.9MB on disk, 108.5MB sheet XML), so uploads failed with
	// RecordFormatException. Uploads are loaded whole into heap (~14x the sheet size), so raising this
	// needs the -Xmx headroom set in the k8s manifests. Override with FYNTRAC_EXCEL_MAX_BYTES.
	static final int DEFAULT_EXCEL_MAX_BYTES = 250_000_000;

	public static void main(String[] args) {
		String excelMaxBytes = System.getenv("FYNTRAC_EXCEL_MAX_BYTES");
		IOUtils.setByteArrayMaxOverride(excelMaxBytes == null || excelMaxBytes.isBlank()
				? DEFAULT_EXCEL_MAX_BYTES : Integer.parseInt(excelMaxBytes.trim()));
		SpringApplication.run(DataloaderApplication.class, args);
	}
}
