package com.reserv.dataloader.entity;

public interface BaseLevelLtd {
    String getId();
    BaseLtd getBalance();
    String getKey(String tenantId);
}
