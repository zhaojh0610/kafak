package com.bfxy.collector.web;

import com.bfxy.collector.util.InputMDC;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author zhaojh
 * @date 2020/10/31 12:47
 */
@RestController
@Slf4j
public class IndexCollector {

    /**
     * [%d{yyyy-MM-dd'T'HH:mm:ss.SSSZZ}]
     * [%level{length=5}]
     * [%thread-%tid]
     * [%logger]
     * [%X{hostName}]
     * [%X{ip}]
     * [%X{applicationName}]
     * [%F,%L,%C,%M]
     * [%m] ## '%ex'%n
     *
     * @return
     */
    @RequestMapping(value = "/index")
    public String index() {
        InputMDC.putMDC();
        log.info("我是一条info日志");
        log.warn("我是一条warn日志");
        log.error("我是一条error日志");
        return "idx";
    }

    @RequestMapping("/err")
    public String error() {
        try {
            int i = 1 / 0;
        } catch (Exception exception) {
            log.error("算术异常");
        }
        return "error";
    }
}
