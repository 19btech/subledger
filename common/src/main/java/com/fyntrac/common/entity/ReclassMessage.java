package com.fyntrac.common.entity;

import com.fyntrac.common.dto.record.Records;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;

/**
 * One attribute change handed from the dataloader's InstrumentAttribute upload to the gl service's
 * reclass processing, grouped by {@code dataKey} (Key.reclassMessageList: tenant + upload run id).
 *
 * <p>This used to travel as a single Memcached list, which was lost whenever the cache was flushed
 * (a TransactionActivity upload flushes all of Memcached as it starts — right after the
 * InstrumentAttribute upload that wrote the list) or evicted, and which could not hold more than one
 * Memcached item (1 MB) of changes. MongoDB keeps it until gl has processed it.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "ReclassMessage")
public class ReclassMessage {
    @Id
    private String id;
    @Indexed
    private String dataKey;
    private Records.InstrumentAttributeReclassMessageRecord message;
    private Date createdAt;
}
