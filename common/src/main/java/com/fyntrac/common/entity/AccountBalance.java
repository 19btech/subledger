package com.fyntrac.common.entity;

import com.fyntrac.common.enums.AccountType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.data.mongodb.core.index.Indexed;

import java.io.Serial;
import java.io.Serializable;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
public class AccountBalance implements Serializable {
    @Serial
    private static final long serialVersionUID = 7617946118568456682L;
    @Indexed(unique = true)
    protected int code;
    @Indexed
    protected int subCode;
    protected String instrumentId;
    protected String attributeId;
    protected AccountType accountType;
    protected String transactionName;
    protected int periodId;
    protected double amount;
    protected String accountNumber;
    protected String accountName;
    protected String accountSubtype;
}