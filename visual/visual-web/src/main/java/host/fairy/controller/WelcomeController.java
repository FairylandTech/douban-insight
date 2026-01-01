/*****************************************************
 * @software: IntelliJ IDEA
 * @author: Lionel Johnson
 * @contact: https://fairy.host
 * @organization: https://github.com/FairylandFuture
 * @datetime: 2026-01-01 16:29:37 UTC+08:00
 ****************************************************/
package host.fairy.controller;

import host.fairy.fairylandfuture.common.web.response.Response;
import host.fairy.service.WelcomeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Welcome controller
 *
 * @author Lionel Johnson
 * @version 1.0
 */
@Slf4j
@RestController
@RequestMapping("/v1/welcome")
public class WelcomeController {
    
    private final WelcomeService welcomeService;
    
    public WelcomeController(WelcomeService welcomeService) {
        this.welcomeService = welcomeService;
    }
    
    /**
     * Welcome endpoint
     *
     * @return Response
     */
    @GetMapping("/hello")
    public Response<Map<String, String>> welcome() {
        log.info("Welcome endpoint accessed! {}", LocalDateTime.now().toString());
        String message = welcomeService.welcome();
        return Response.success(200, message, Map.of("message", message));
    }
}
