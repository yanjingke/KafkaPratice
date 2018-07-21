import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class TestJedis {
    private Jedis jedis;
    @Before
    public void before() {
        jedis = new Jedis("hadoop");
    }
    @Test
    public void test3() {
        Map<String, String> map = new HashMap<String, String>();
        map.put("name", "fujianchao");
        map.put("password", "123");
        map.put("age", "12");
        // 存入一个map
        jedis.hmset("user", map);

        // map key的个数
        System.out.println("map的key的个数" + jedis.hlen("user"));

        // map key
        System.out.println("map的key" + jedis.hkeys("user"));

        // map value
        System.out.println("map的value" + jedis.hvals("user"));

        // (String key, String... fields)返回值是一个list
        List<String> list = jedis.hmget("user", "age");
//        System.out.println(l);
//
//        // 删除map中的某一个键 的值 password
//        // 当然 (key, fields) 也可以是多个fields
//        jedis.hdel("user", "age");
//
//        System.out.println("删除后map的key" + jedis.hkeys("user"));

    }

    /**
     * list
     */
    @Test
    public void test4() {
        jedis.lpush("list1", "aa");
        jedis.lpush("list1", "bb");
        jedis.lpush("list1", "cc");
        System.out.println(jedis.lrange("list1", 0, -1));
        System.out.println(jedis.lrange("list1", 0, 1));
        System.out.println(jedis.lpop("list1")); // 栈顶
        jedis.del("list1");
    }
    @Test
    public void test5() {
        Map<String,String> map =  new HashMap<>();
        map.put("b","3c");
        map.put("c","phone");
        map.put("s","121");
        map.put("p","iphone");
        jedis.hmset("order", map);

    }
}
