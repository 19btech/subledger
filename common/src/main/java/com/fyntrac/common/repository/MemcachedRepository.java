package com.fyntrac.common.repository;

import lombok.extern.slf4j.Slf4j;
import net.spy.memcached.transcoders.SerializingTranscoder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Repository
@Slf4j
public class MemcachedRepository {
    protected final MemcachedClient memcachedClient;
    @Value("${fyntrac.cache.timeout}")
    protected int cacheTimeOut;
    @Autowired
    public MemcachedRepository(MemcachedClient memcachedClient) {
        this.memcachedClient = memcachedClient;
    }

    public <T> T getFromCache(String key, Class<T> clazz) {
        try {
            return clazz.cast(memcachedClient.get(key));
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public Object getFromCache(String key) {
        try {
            return memcachedClient.get(key);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public <T> OperationFuture<Boolean> replaceInCache(String key, T object, int expirationTimeInSeconds) {
        try {
            return memcachedClient.replace(key, expirationTimeInSeconds, object);
        } catch (Exception e) {
            log.error("Error replacing object in cache", e);
            throw new RuntimeException(e);
        }
    }

    public <T>  OperationFuture<Boolean> replaceInCache(String key, T object) {
        return this.replaceInCache(key,object, cacheTimeOut);
    }

    public <T> OperationFuture<Boolean> appendInCache(String key, T object) {
        try {
            return memcachedClient.append(key,  object);
        } catch (Exception e) {
            log.error("Error appending object in cache", e);
            throw new RuntimeException(e);
        }
    }

    public <T> OperationFuture<Boolean> putInCache(String key, T object, int expirationTimeInSeconds) {
        try {
            return memcachedClient.set(key, expirationTimeInSeconds, object);
        } catch (Exception e) {
            log.error("Error putting object in cache", e);
            throw new RuntimeException(e);
        }
    }

    public <T> OperationFuture<Boolean> putCollectionInCache(String key, T object, int expirationTimeInSeconds) {
        try {
            // Use a SerializingTranscoder for proper serialization
            SerializingTranscoder transcoder = new SerializingTranscoder();
            return memcachedClient.set(key, expirationTimeInSeconds, object, transcoder);
        } catch (Exception e) {
            log.error("Error putting object in cache", e);
            throw new RuntimeException(e);
        }
    }
    public <T> OperationFuture<Boolean> putInCache(String key, T object) {
        try {
            return memcachedClient.set(key, 0, object);
        } catch (Exception e) {
            // It's generally a bad practice to just print the stack trace and continue.
            // Instead, consider logging the exception and re-throwing it, or returning a failed future.
            log.error("Error putting object in cache", e);
            throw new RuntimeException(e);
        }
    }
    public <T> void putInCache(String tenantId, List<? extends T> items) {
        for(Object item : items) {
            putInCache(tenantId + item.hashCode(), item);
        }
    }

    public void flush(int delay) {
        memcachedClient.flush(delay);
    }
    public boolean ifExists(String key) {
        try {
            return memcachedClient.get(key) != null;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public void delete(String key) throws ExecutionException, InterruptedException {
        Object cachedObject = memcachedClient.get(key);
        if (cachedObject != null) {
            boolean wasDeleted = memcachedClient.delete(key).get();
            if (wasDeleted) {
                System.out.println("Object deleted successfully.");
            } else {
                System.out.println("Failed to delete the object.");
            }
        } else {
            System.out.println("Object does not exist in cache.");
        }
    }
}