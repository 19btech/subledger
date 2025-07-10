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
@Builder
public class BaseLtd implements Serializable {
    private static final long serialVersionUID = -7956532636381889893L;

    @NumberFormat(pattern = "#.####")
    private BigDecimal activity;

    @NumberFormat(pattern = "#.####")
    private BigDecimal beginningBalance;

    @NumberFormat(pattern = "#.####")
    private BigDecimal endingBalance;

    public BaseLtd(BigDecimal activity, BigDecimal beginningBalance, BigDecimal endingBalance) {
        setActivity(activity);
        setBeginningBalance(beginningBalance);
        setEndingBalance(endingBalance);
    }

    public void setActivity(BigDecimal activity) {
        this.activity = activity != null ? activity.setScale(4, RoundingMode.HALF_UP) : null;
    }

    public void setBeginningBalance(BigDecimal beginningBalance) {
        this.beginningBalance = beginningBalance != null ? beginningBalance.setScale(4, RoundingMode.HALF_UP) : null;
    }

    public void setEndingBalance(BigDecimal endingBalance) {
        this.endingBalance = endingBalance != null ? endingBalance.setScale(4, RoundingMode.HALF_UP) : null;
    }
}
