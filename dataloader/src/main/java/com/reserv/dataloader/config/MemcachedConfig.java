package com.reserv.dataloader.config;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class MemcachedConfig {

    @Value("${memcached.host}")
    private String memcachedHost;

    @Value("${memcached.port}")
    private int memcachedPort;

    @Bean
    public MemcachedClient memcachedClient() throws IOException {
        ConnectionFactory connectionFactory = new ConnectionFactoryBuilder()
                .setOpTimeout(5000)
                .setMaxReconnectDelay(14)
                .build();
        List<InetSocketAddress> inetSocketAddresses = new ArrayList<>(0);
        inetSocketAddresses.add(new InetSocketAddress(memcachedHost, memcachedPort));
        return new MemcachedClient(connectionFactory, inetSocketAddresses );
    }
}

