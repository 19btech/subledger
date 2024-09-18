package com.reserv.dataloader.entity;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serial;
import java.io.Serializable;

@Slf4j
@Data
@Builder
public class BaseLtd implements Serializable {
    @Serial
    private static final long serialVersionUID = -7956532636381889893L;
    private double activity;
    private double beginningBalance;
    public double getLtdBalance() {
        return this.beginningBalance + this.activity;
    }
}
