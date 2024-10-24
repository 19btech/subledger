package com.fyntrac.common.entity;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serial;
import java.io.Serializable;

@Slf4j
@Data
@lombok.Builder(builderClassName = "BaseLtdBuilder")
public class BaseLtd implements Serializable {
    @Serial
    private static final long serialVersionUID = -7956532636381889893L;
    private double activity;
    private double beginningBalance;
    private double endingBalance;

    public BaseLtd(double activity, double beginningBalance, double endingBalance) {
        this.activity = activity;
        this.beginningBalance = beginningBalance;
        this.endingBalance = endingBalance;
    }

    // Custom builder configuration to update endingBalance
    public static class BaseLtdBuilder {
        public BaseLtdBuilder activity(double activity) {
            this.activity = activity;
            return this;
        }

        public BaseLtdBuilder beginningBalance(double beginningBalance) {
            this.beginningBalance = beginningBalance;
            return this;
        }

        public BaseLtd build() {
            double endingBalance = this.beginningBalance + this.activity;
            return new BaseLtd(this.activity, this.beginningBalance, endingBalance);
        }
    }
}
