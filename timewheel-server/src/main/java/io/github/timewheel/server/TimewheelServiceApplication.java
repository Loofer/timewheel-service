package io.github.timewheel.server;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "io.github.timewheel")
public class TimewheelServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(TimewheelServiceApplication.class, args);
    }
}
