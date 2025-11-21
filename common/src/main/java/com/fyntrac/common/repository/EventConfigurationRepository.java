package com.fyntrac.common.repository;


import com.fyntrac.common.entity.EventConfiguration;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface EventConfigurationRepository extends MongoRepository<EventConfiguration, String> {

    Optional<EventConfiguration> findByEventId(String eventId);


    List<EventConfiguration> findByIsActive(Boolean isActive);

    List<EventConfiguration> findByIsActiveOrderByPriorityAsc(Boolean isActive);

    @Query("{ 'eventId': ?0, 'isActive': true }")
    Optional<EventConfiguration> findActiveByEventId(String eventId);

    boolean existsByEventId(String eventId);

    @Query("{ 'eventId': ?0, 'isActive': true }")
    boolean existsActiveByEventIdAndTenantId(String eventId);

    @Query("{ 'isDeleted': false }")
    List<EventConfiguration> findAllUndeleted();

    @Query(value = "{ 'isActive': true }", fields = "{ 'eventId': 1, 'eventName': 1, 'priority': 1 }")
    List<EventConfiguration> findBasicInfo();

    @Query(fields = "{ 'eventId': 1, 'eventName': 1, 'priority': 1 }")
    List<EventConfiguration> findByIsActive(boolean isActive);
}
