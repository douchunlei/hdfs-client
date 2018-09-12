package com.hdfs.client.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class HdfsClientApplication {

	public static void main(String[] args) {
		SpringApplication.run(HdfsClientApplication.class, args);
	}
}
