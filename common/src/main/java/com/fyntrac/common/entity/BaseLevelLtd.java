package com.fyntrac.common.entity;

public interface BaseLevelLtd {
    String getId();
    BaseLtd getBalance();
    String getKey(String tenantId);
}
