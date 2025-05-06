package com.fyntrac.common.entity;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AggregationMap implements Serializable{

    @Serial
    private static final long serialVersionUID = -3326921991344881948L;
    private Map<String, AggregationRequest> aggregationRequestMap = new HashMap<>(0);

    public void add(String key, AggregationRequest aggregationRequest){
        this.aggregationRequestMap.put(key, aggregationRequest);
    }

    public AggregationRequest get(String key) {
        return this.aggregationRequestMap.get(key);
    }
}
