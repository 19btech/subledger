package com.reserv.dataloader.entity;

import lombok.Data;

import java.io.Serial;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.Queue;

public class AggregationQueue  implements Serializable{

    @Serial
    private static final long serialVersionUID = -3326921991344881948L;
    private Queue<AggregationRequest> aggregationQueue = new LinkedList<>();

    public void add(AggregationRequest aggregationRequest){
        this.aggregationQueue.add(aggregationRequest);
    }

    public AggregationRequest poll() {
        return this.aggregationQueue.poll();
    }
}
