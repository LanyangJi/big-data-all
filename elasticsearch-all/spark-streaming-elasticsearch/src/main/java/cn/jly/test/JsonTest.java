package cn.jly.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.*;

/**
 * @author lanyangji
 * @date 2021/4/15 下午 4:53
 * @packageName cn.jly.test
 * @className JsonTest
 */
public class JsonTest {
    public static void main(String[] args) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();

        Person person = new Person(1001L, "tom", 23, "tom@qq.com");
        String s = objectMapper.writeValueAsString(person);
        System.out.println("s = " + s);

        System.out.println("---------------");
        InputStream resourceAsStream = JsonTest.class.getClassLoader().getResourceAsStream("person.json");
        assert resourceAsStream != null;
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(resourceAsStream))) {
            String str;
            while ((str = bufferedReader.readLine()) != null) {
                Person resPerson = objectMapper.readValue(str, Person.class);
                System.out.println("resPerson = " + resPerson);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Person {
        private Long id;
        private String name;
        private Integer age;
        private String email;
    }
}
