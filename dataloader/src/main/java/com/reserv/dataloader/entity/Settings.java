package com.reserv.dataloader.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "Settrings")
public class Settings {
    @Id
    private String id;
    private String homeCurrency;
    private String glamFields;
    private Date fiscalPeriodStartDate;
    private Date reportingPeriod;
    private boolean isRestatementMode;

}