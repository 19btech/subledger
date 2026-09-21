package com.fyntrac.common.entity;

import com.fyntrac.common.enums.ErrorCategory;
import com.fyntrac.common.enums.ErrorCode;
import com.fyntrac.common.enums.ErrorType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;
import java.util.Date;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "Errors")
// Covers ErrorsRepository.findByInstrumentId/findByInstrumentIdAndAttributeId as index prefixes,
// plus the full instrumentId+attributeId+postingDate combination. No index existed on this
// collection at all before.
@CompoundIndex(def = "{'instrumentId': 1, 'attributeId': 1, 'postingDate': 1}", name = "Errors_instrument_attribute_postingdate_index")
public class Errors implements Serializable {
    @Serial
    private static final long serialVersionUID = -7374000552564642342L;

    @Id
    private String id;
    private String errorId;
    private ErrorCode code;
    private Date executionDate;
    private String instrumentId;
    private String attributeId;
    private String modelId;
    private String stacktrace;
    private boolean isWarning;
    private String sourceTable;
    private String sourceColumn;
    private Long rowNum;
    private ErrorCategory errorCategory;
    private ErrorType errorType;
    private String message;
    private String jobId;
    private Date createdTimestamp;
    private Date postingDate;
}
