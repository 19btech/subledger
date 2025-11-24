package com.fyntrac.common.service;

import com.fyntrac.common.entity.Event;
import com.fyntrac.common.entity.Option;
import com.fyntrac.common.repository.EventRepository;
import com.fyntrac.common.utils.DateUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.text.ParseException;
import java.util.*;

@Service
@Transactional
@Slf4j
public class EventService {

    private final EventRepository eventRepository;
    private final DataService dataService;

    public EventService(EventRepository eventRepository,
                        DataService dataService) {
        this.eventRepository = eventRepository;
        this.dataService = dataService;
    }

    public List<Option> getDistinctPostingDates(String instrumentId) throws ParseException {
        Query query = new Query(Criteria.where("instrumentId").is(instrumentId));
        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate();
        List<Integer> postingDates = mongoTemplate.query(Event.class)
                .distinct("postingDate")
                .matching(query)
                .as(Integer.class)
                .all();

        List<Option> dates = new ArrayList<>(0);
        for(Integer postingDate :  postingDates) {
            Date utcDate = DateUtil.convertIntDateToUtc(postingDate);
            String strDate = DateUtil.formatDateToString(utcDate, "MM/dd/yyyy");
            Option option = Option.builder().label(strDate)
                    .value(postingDates.toString()).build();
            dates.add(option);
        }

        return dates;
    }

    public List<Option> getDistinctPostingDates() throws ParseException {
        MongoTemplate mongoTemplate = this.dataService.getMongoTemplate();
        List<Integer> postingDates = mongoTemplate.query(Event.class)
                .distinct("postingDate")
                .as(Integer.class)
                .all();

        List<Option> dates = new ArrayList<>(0);
        for(Integer postingDate :  postingDates) {
            Date utcDate = DateUtil.convertIntDateToUtc(postingDate);
            String strDate = DateUtil.formatDateToString(utcDate, "MM/dd/yyyy");
            Option option = Option.builder().label(strDate)
                    .value(postingDate.toString()).build();
            dates.add(option);
        }

        return dates;
    }

    List<Event> getEvents(String instrumentId, Integer postingDate) {
        return this.eventRepository.findByPostingDateAndInstrumentId(postingDate, instrumentId);
    }

    Map<String, Map<String, Object>> getEventValueMap(String instrumentId, Integer postingDate) {
        List<Event> events = this.getEvents(instrumentId, postingDate);

        Map<String, Map<String, Object>> valueMap = new HashMap<>(0);

        for(Event event : events) {
            String eventId = event.getEventId();
            Map<String,Map<String, Object>> values = event.getEventDetail().getValues();
            Map<String, Object> tmpValueMap = new HashMap(0);
            for(Map<String, Object> map : values.values()) {
                tmpValueMap.putAll(map);
            }

            valueMap.put(eventId, tmpValueMap);
        }

        return valueMap;
    }

    Map<String, Map<String, Object>> getEventValueMap(List<Event> events) {

        Map<String, Map<String, Object>> valueMap = new HashMap<>(0);

        for(Event event : events) {
            String eventId = event.getEventId();
            Map<String,Map<String, Object>> values = event.getEventDetail().getValues();
            Map<String, Object> tmpValueMap = new HashMap(0);
            for(Map<String, Object> map : values.values()) {
                tmpValueMap.putAll(map);
            }

            valueMap.put(eventId, tmpValueMap);
        }

        return valueMap;
    }
}
