package com.fyntrac.gl.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "StageGeneralLedgerEntry")
public class StageGeneralLedgerEntry {
    String transactionName;
}
