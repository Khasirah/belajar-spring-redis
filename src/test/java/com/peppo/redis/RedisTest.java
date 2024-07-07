package com.peppo.redis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.domain.geo.Metrics;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

@SpringBootTest
public class RedisTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void testRedisTemplate() {
        assertNotNull(redisTemplate);
        // result tidak null
        // artinya StringRedisTemplate akan di inject secara otomatis oleh spring boot
    }

    @Test
    void testString() throws InterruptedException {
        ValueOperations<String, String> operations = redisTemplate.opsForValue();

        operations.set("name", "haris", Duration.ofSeconds(2));
        assertEquals("haris", operations.get("name"));

        Thread.sleep(Duration.ofSeconds(3));
        assertNull(operations.get("name"));
    }

    @Test
    void testList() {
        ListOperations<String, String> operations = redisTemplate.opsForList();

        operations.rightPush("name", "ahmad");
        operations.rightPush("name", "haris");
        operations.rightPush("name", "kurniawan");

        assertEquals("ahmad", operations.leftPop("name"));
        assertEquals("haris", operations.leftPop("name"));
        assertEquals("kurniawan", operations.leftPop("name"));
    }

    @Test
    void testSet() {
        SetOperations<String, String> operations = redisTemplate.opsForSet();

        operations.add("students", "ahmad");
        operations.add("students", "ahmad");
        operations.add("students", "haris");
        operations.add("students", "haris");
        operations.add("students", "kurniawan");
        operations.add("students", "kurniawan");

        Set<String> students = operations.members("students");

        assertNotNull(students);
        assertEquals(3, students.size());
        assertThat(students, hasItems("ahmad", "haris", "kurniawan"));
    }

    @Test
    void testZSet() {
        ZSetOperations<String, String> operations = redisTemplate.opsForZSet();
        operations.add("score", "haris", 100);
        operations.add("score", "sirah", 83);
        operations.add("score", "peppo", 92);

        assertEquals("haris", operations.popMax("score").getValue());
        assertEquals("peppo", operations.popMax("score").getValue());
        assertEquals("sirah", operations.popMax("score").getValue());
    }

    @Test
    void testHash() {
        HashOperations<String, Object, Object> operations = redisTemplate.opsForHash();
        operations.put("user1", "id", "1");
        operations.put("user1", "name", "Haris");
        operations.put("user1", "email", "haris@haris.com");

        Map<Object, Object> user2= new HashMap<>();
        user2.put("id", "2");
        user2.put("name", "Kurniawan");
        user2.put("email", "kurniawan@haris.com");
        operations.putAll("user2", user2);

        assertEquals("1", operations.get("user1", "id"));
        assertEquals("Haris", operations.get("user1", "name"));
        assertEquals("haris@haris.com", operations.get("user1", "email"));
        assertEquals("Kurniawan", operations.get("user2", "name"));

        redisTemplate.delete("user1");
    }

    @Test
    void testGeo() {
        GeoOperations<String, String> operations = redisTemplate.opsForGeo();

        operations.add("sellers", new Point(106.822812, -6.181606), "Toko A");
        operations.add("sellers", new Point(106.824483,-6.182035), "Toko B");

        Distance distance = operations.distance("sellers", "Toko A", "Toko B", Metrics.KILOMETERS);

        assertEquals(new Distance(0.1905, Metrics.KILOMETERS), distance);

        // get all sellers according to point
        GeoResults<RedisGeoCommands.GeoLocation<String>> sellers =
                operations.search("sellers", new Circle(
                new Point(106.823232, -6.182310),
                new Distance(5, Metrics.KILOMETERS)
        ));

        assertEquals(2, sellers.getContent().size());
        assertEquals("Toko A", sellers.getContent().get(0).getContent().getName());
        assertEquals("Toko B", sellers.getContent().get(1).getContent().getName());
    }

    @Test
    void testHyperLogLog() {
        HyperLogLogOperations<String, String> operations = redisTemplate.opsForHyperLogLog();

        operations.add("traffics", "haris", "kurniawan", "ahmad");
        operations.add("traffics", "haris", "popi", "lestari");
        operations.add("traffics", "popi", "nopiyanti", "haris");

        assertEquals(6L, operations.size("traffics"));
    }

    @Test
    void testTransaction() {
        redisTemplate.execute(new SessionCallback<Object>() {

            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();

                operations.opsForValue().set("test1", "haris", Duration.ofSeconds(2));
                operations.opsForValue().set("test2", "kurniawan", Duration.ofSeconds(2));

                operations.exec();
                return null;
            }
        });

        assertEquals("haris", redisTemplate.opsForValue().get("test1"));
        assertEquals("kurniawan", redisTemplate.opsForValue().get("test2"));
    }

    @Test
    void testPipeline() {
        List<Object> list = redisTemplate.executePipelined(new SessionCallback<>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.opsForValue().set("test1", "haris");
                operations.opsForValue().set("test2", "kurniawan");
                operations.opsForValue().set("test3", "popi");
                operations.opsForValue().set("test4", "lestari");
                return null;
            }
        });

        assertThat(list, hasSize(4));
        assertThat(list, hasItems(true));
        assertThat(list, not(hasItems(false)));
    }

    @Test
    void testPublishStream() {
        var operations = redisTemplate.opsForStream();
        var record = MapRecord.create("stream-1", Map.of(
                "name", "ahmad haris kurniawan",
                "address", "indonesia"
        ));

        for (int i = 0; i < 10; i++) {
            operations.add(record);
        }
    }

    @Test
    void testSubscribeStream() {
        var operations = redisTemplate.opsForStream();
        try {
            operations.createGroup("stream-1", "sample-group");
        } catch (RedisSystemException exception) {
            // group already exist
        }

        List<MapRecord<String, Object, Object>> records = operations.read(Consumer.from("sample-group", "sample-1"),
                StreamOffset.create("stream-1", ReadOffset.lastConsumed()));

        for (MapRecord<String, Object, Object> record : records) {
            System.out.println(record);
        }
    }

    @Test
    void testPubSub() {
        redisTemplate.getConnectionFactory().getConnection().subscribe(new MessageListener() {
            @Override
            public void onMessage(Message message, byte[] pattern) {
                String event = new String(message.getBody());
                System.out.println("Received message: " + event);
            }
        }, "my-channel".getBytes());

        for (int i = 0; i < 10; i++) {
            redisTemplate.convertAndSend("my-channel", "Hello world : " + i);
        }
    }
}
