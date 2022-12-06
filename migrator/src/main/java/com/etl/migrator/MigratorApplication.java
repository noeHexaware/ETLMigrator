package com.etl.migrator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@EnableKafka
@SpringBootApplication
public class MigratorApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(MigratorApplication.class, args);
	}
}