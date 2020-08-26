package com.ibm.nestor;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StatusController {
    @RequestMapping("/status/ping")
    public String index() {
        return "OK";
    }
}
