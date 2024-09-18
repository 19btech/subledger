package com.reserv.dataloader.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "AccountingPeriod")
public class AccountingPeriod implements Serializable {
    @Serial
    private static final long serialVersionUID = 8712613806683864992L;
    private Date startDate;
    private int calendarMonth;
    private int year;
    private int days;
    private Date endDate;
    private int fiscalPeriod;
    private String period;
    private int periodId;
    private int status;
    private String previousAccountingPeriod;
}
