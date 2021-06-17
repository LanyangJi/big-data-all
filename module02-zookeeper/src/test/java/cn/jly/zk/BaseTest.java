package cn.jly.zk;

import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.RandomUtil;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lanyangji
 * @date 2021/5/8 上午 11:10
 * @packageName cn.jly.zk
 * @className BaseTest
 */
public class BaseTest {
    public static AtomicInteger id_generator = new AtomicInteger(1000);

    private static List<String> jobs = new ArrayList<>();
    private static List<Integer> managerIds = new ArrayList<>();

    static {
        jobs.add("CLERK");
        jobs.add("SALESMAN");
        jobs.add("MANAGER");
        jobs.add("PRESIDENT");
        jobs.add("ANALYST");

        managerIds.add(id_generator.getAndIncrement());
        managerIds.add(id_generator.getAndIncrement());
        managerIds.add(id_generator.getAndIncrement());
        managerIds.add(id_generator.getAndIncrement());
    }

    @org.junit.Test
    public void name() {
        for (int i = 0; i < 20; i++) {
            Calendar calendar = Calendar.getInstance();
            calendar.set(2013, 12, 12);
            Emp emp = new Emp();
            emp.setEmpNo(id_generator.getAndIncrement());
            emp.setName(RandomUtil.randomString(5));
            emp.setJob(RandomUtil.randomEle(jobs));
            emp.setManagerId(RandomUtil.randomEle(managerIds));
            emp.setHireDate(
                    DateUtil.format(
                            RandomUtil.randomDate(calendar.getTime(), DateField.MONTH, 1, 5)
                            , "yyyy-MM-dd")
            );
            emp.setSalary(RandomUtil.randomDouble(7000d, 10000d));
            emp.setComm(emp.job.equals("MANAGER") ? RandomUtil.randomDouble(100d, 200d) : 0d);
            emp.setDeptId(RandomUtil.randomInt(1, 5));

            System.out.println(emp);
        }
    }

    public static class Emp {
        private Integer empNo;
        private String name;
        private String job;
        private Integer managerId;
        private String hireDate;
        private Double salary;
        private Double comm;
        private Integer deptId;

        @Override
        public String toString() {
            return String.join("\t",
                    this.empNo.toString(),
                    this.name,
                    this.job,
                    this.managerId.toString(),
                    this.hireDate,
                    this.salary.toString(),
                    this.comm.toString(),
                    this.deptId.toString());
        }

        public Integer getEmpNo() {
            return empNo;
        }

        public void setEmpNo(Integer empNo) {
            this.empNo = empNo;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getJob() {
            return job;
        }

        public void setJob(String job) {
            this.job = job;
        }

        public Integer getManagerId() {
            return managerId;
        }

        public void setManagerId(Integer managerId) {
            this.managerId = managerId;
        }

        public String getHireDate() {
            return hireDate;
        }

        public void setHireDate(String hireDate) {
            this.hireDate = hireDate;
        }

        public Double getSalary() {
            return salary;
        }

        public void setSalary(Double salary) {
            this.salary = salary;
        }

        public Double getComm() {
            return comm;
        }

        public void setComm(Double comm) {
            this.comm = comm;
        }

        public Integer getDeptId() {
            return deptId;
        }

        public void setDeptId(Integer deptId) {
            this.deptId = deptId;
        }
    }
}
