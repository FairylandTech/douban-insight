/*****************************************************
 * @software: IntelliJ IDEA
 * @author: Lionel Johnson
 * @contact: https://fairy.host
 * @organization: https://github.com/FairylandFuture
 * @datetime: 2026-01-01 16:39:48 UTC+08:00
 ****************************************************/
package host.fairy.service.impl;

import host.fairy.service.WelcomeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * @author Lionel Johnson
 * @version 1.0
 */
@Slf4j
@Service
public class WelcomeServiceImpl implements WelcomeService {
    /**
     * Welcome message
     *
     * @return welcome message
     */
    @Override
    public String welcome() {
        log.info("WelcomeServiceImpl: welcome method called");
        return "Welcome to web service!";
    }
}
