package cn.jly.bigdata.flink_advanced.datastream.beans;

import java.util.Objects;

public class TblUser {
        private Integer id;
        private String name;
        private Integer age;

        public TblUser() {
        }

        public TblUser(Integer id, String name, Integer age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TblUser tblUser = (TblUser) o;
            return Objects.equals(id, tblUser.id) && Objects.equals(name, tblUser.name) && Objects.equals(age, tblUser.age);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, age);
        }

        @Override
        public String toString() {
            return "TblUser{" +
                    "id=" + id +
                    ", name='" + name + '\'' +
                    ", age=" + age +
                    '}';
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }