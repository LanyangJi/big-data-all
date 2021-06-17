package cn.jly.boot;

import cn.hutool.core.lang.Console;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author lanyangji
 * @date 2021年5月15日 20:06:52
 */
@RestController
@SpringBootApplication
public class Module00SpringbootApplication {

    public static void main(String[] args) {
        SpringApplication.run(Module00SpringbootApplication.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(ApplicationContext context) {
        return args -> {
            int beanDefinitionCount = context.getBeanDefinitionCount();
            Console.log("spring boot容器中的bean个数: {}", beanDefinitionCount);
        };
    }

    @GetMapping("/greet")
    public String greet(String name) {
        return String.join(",", "hello", name);
    }

}
