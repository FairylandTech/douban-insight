/*****************************************************
 * @software: IntelliJ IDEA
 * @author: Lionel Johnson
 * @contact: https://fairy.host
 * @organization: https://github.com/FairylandFuture
 * @datetime: 2026-01-01 16:21:46 UTC+08:00
 ****************************************************/
package host.fairy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author Lionel Johnson
 * @version 1.0
 */
@SpringBootApplication(scanBasePackages = "host.fairy")
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
