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
