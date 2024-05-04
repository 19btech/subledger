package com.reserv.dataloader.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDate;
import java.time.Period;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "AccountingPeriod")
public class AccountingPeriod {
    private LocalDate startDate;
    private int calendarMonth;
    private int year;
    private int days;
    private LocalDate endDate;
    private int fiscalPeriod;
    private String period;
    private int status;
}
