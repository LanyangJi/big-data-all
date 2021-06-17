package cn.jly.boot.springdataelasticsearch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author lanyangji
 * @date 2021年4月14日09:25:10
 */
@RestController
@SpringBootApplication
public class SpringDataElasticsearchApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringDataElasticsearchApplication.class, args);
    }

    @GetMapping("/greet")
    public String greeting(String name) {
        return "hello, " + name;
    }
}
