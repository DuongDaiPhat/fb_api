package com.fbapi.backendapi;

import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@SpringBootApplication
public class BackendApiApplication {

    public static void main(String[] args) {
        loadDotenv();
        SpringApplication.run(BackendApiApplication.class, args);
    }

    private static void loadDotenv() {
        try {
            // Load from root of project (3 levels up from backend-api: fb_api/services/backend-api -> fb_api -> fb_api -> root)
            // The root is d:\hoc tap\API\Facebook API\fb_api
            // The backend-api is at d:\hoc tap\API\Facebook API\fb_api\fb_api\services\backend-api
            Path dotenvPath = Paths.get("../../../.env");
            if (Files.exists(dotenvPath)) {
                Dotenv dotenv = Dotenv.configure()
                        .directory("../../../")
                        .filename(".env")
                        .load();
                dotenv.entries().forEach(entry -> {
                    if (System.getProperty(entry.getKey()) == null) {
                        System.setProperty(entry.getKey(), entry.getValue());
                    }
                });
            } else if (Files.exists(Paths.get(".env"))) {
                Dotenv dotenv = Dotenv.configure().load();
                dotenv.entries().forEach(entry -> {
                    if (System.getProperty(entry.getKey()) == null) {
                        System.setProperty(entry.getKey(), entry.getValue());
                    }
                });
            }
        } catch (Exception e) {
            System.out.println("No .env file found or failed to load, falling back to environment variables.");
        }
    }
}
