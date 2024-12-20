package com.fyntrac.common.entity;

import com.fyntrac.common.enums.BatchStatus;
import com.fyntrac.common.enums.BatchType;
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
@Document(collection = "Batch")
public class Batch implements Serializable {
    @Serial
    private static final long serialVersionUID = 2725921410661737139L;
    long id;
    Date uploadDate;
    BatchType batchType;
    BatchStatus batchStatus;
}
