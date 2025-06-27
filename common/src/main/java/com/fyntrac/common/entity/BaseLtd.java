package com.fyntrac.common.entity;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.NumberFormat;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
@Data
@Builder(builderClassName = "BaseLtdBuilder")
public class BaseLtd implements Serializable {
    private static final long serialVersionUID = -7956532636381889893L;

    @NumberFormat(pattern = "#.####")
    private BigDecimal activity;
    @NumberFormat(pattern = "#.####")
    private BigDecimal beginningBalance;
    @NumberFormat(pattern = "#.####")
    private BigDecimal endingBalance;

    public BaseLtd(BigDecimal activity, BigDecimal beginningBalance, BigDecimal endingBalance) {
        this.activity = activity.setScale(4, RoundingMode.HALF_UP);
        this.beginningBalance = beginningBalance.setScale(4, RoundingMode.HALF_UP);
        this.endingBalance = endingBalance.setScale(4, RoundingMode.HALF_UP);
    }

    // Custom builder configuration to update endingBalance
    public static class BaseLtdBuilder {
        public BaseLtdBuilder activity(BigDecimal activity) {
            this.activity = activity.setScale(4, RoundingMode.HALF_UP);
            return this;
        }

        public BaseLtdBuilder beginningBalance(BigDecimal beginningBalance) {
            this.beginningBalance = beginningBalance.setScale(4, RoundingMode.HALF_UP);
            return this;
        }

        public BaseLtd build() {
            BigDecimal endingBalance = this.beginningBalance.add(this.activity).setScale(4, RoundingMode.HALF_UP);
            return new BaseLtd(this.activity, this.beginningBalance, endingBalance);
        }
    }
}
