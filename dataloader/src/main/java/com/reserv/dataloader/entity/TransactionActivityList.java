package com.reserv.dataloader.entity;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TransactionActivityList implements Serializable {
    @Serial
    private static final long serialVersionUIDL=-3326921991344871948L;;
    private List<String> list = new ArrayList<>(0);

    public void add(String key){
        this.list.add(key);
    }

    public List<String> get() {
        return this.list;
    }
}
