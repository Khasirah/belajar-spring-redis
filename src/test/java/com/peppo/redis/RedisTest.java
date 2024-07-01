package com.peppo.redis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Circle;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.*;
import org.springframework.data.redis.domain.geo.Metrics;

import java.time.Duration;
import java.util.HashMap;
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
}
