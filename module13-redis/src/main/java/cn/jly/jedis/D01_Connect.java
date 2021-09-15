package cn.jly.jedis;

import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * @author jilanyang
 * @date 2021/9/14 22:45
 */
public class D01_Connect {
	public static void main(String[] args) {
		Jedis jedis = new Jedis("192.168.172.201", 6379);

		// 测试链接
		String ping = jedis.ping();
		System.out.println("ping = " + ping);

		// 查询所有的key
		Set<String> keys = jedis.keys("*");
		for (String key : keys) {
			System.out.println("key = " + key);
		}

		jedis.close();
	}
}
