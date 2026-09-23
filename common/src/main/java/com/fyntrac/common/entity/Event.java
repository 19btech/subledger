package com.fyntrac.common.entity;

import com.fyntrac.common.enums.EventStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.validation.constraints.NotNull;
import java.io.Serial;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "EventHistory")
public class Event {
    @Serial
    private static final long serialVersionUID = -6419191618618167082L;

    /**
     * instrumentId of a reference-table event (triggerSource "reference_table", e.g. SSP_RULE ->
     * Accounting_Policy). A reference table has no instrument tie, so its event is written ONCE per
     * (eventId, postingDate) under this sentinel instead of once per instrument — previously every
     * instrument got its own byte-identical copy (77% of EventHistory on Hearst). Every reader that
     * loads an instrument's events must also load the docs under this id; readers that count or
     * page instruments must exclude it. The literal is repeated in EventRepository @Query strings
     * and in fyntrac-py-model's pulsar/manager.py — keep them in sync.
     */
    public static final String SHARED_REFERENCE_INSTRUMENT_ID = "__SHARED_REFERENCE__";

    @Id
    private String id;
    @NotNull
    @Indexed
    private String instrumentId;
    @NotNull
    @NotNull
    @Indexed
    private String eventId;
    @Indexed
    private String eventName;
    @NotNull
    @Indexed
    private Integer postingDate;
    @NotNull
    @Indexed
    private Integer effectiveDate;
    @NotNull
    @Indexed
    private Integer lastPlayedPostingDate;
    @NotNull
    @Indexed
    private Integer priority;
    @NotNull
    private EventStatus status;
    private EventDetail eventDetail;

}
