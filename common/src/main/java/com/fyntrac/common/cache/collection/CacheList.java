package com.fyntrac.common.cache.collection;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CacheList <T> implements Serializable {
    @Serial
    private static final long serialVersionUID = -6503013787584293223L;
    private List<T> list;

    public CacheList(){
        this.list= new ArrayList<>(0);
    }

    public void add(T o) {
        this.list.add(o);
    }

    public List<T> getList() {
        return this.list;
    }
}
