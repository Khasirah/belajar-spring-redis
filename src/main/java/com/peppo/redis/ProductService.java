package com.peppo.redis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ProductService {

    @Cacheable(value = "products", key = "#productId")
    public Product getProduct(String productId) {
        log.info("getProduct productId={}", productId);
        return Product.builder().id(productId).name("contoh").price(10L).build();
    }

    @CachePut(value = "products", key = "#product.id")
    public Product saveProduct(Product product) {
        log.info("saveProduct product={}", product);
        return product;
    }

    @CacheEvict(value = "products", key = "#productId")
    public void removeProduct(String productId) {
        log.info("removeProduct productId={}", productId);
    }
}
