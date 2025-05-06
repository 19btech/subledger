package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import java.io.Serial;
import java.io.Serializable;
import java.util.Objects;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "ExecutionState")
public class ExecutionState implements Serializable {
    @Serial
    private static final long serialVersionUID = -6372227210157798321L;

    @Id
    private String id;
    private Integer executionDate;
    private Integer lastExecutionDate;
    private Integer activityPostingDate;
    private Integer lastActivityPostingDate;
    private Integer lastActivityEffectiveDate;

    @Override
    public String toString() {
        return "{" +
                "\"executionDate\":" + (executionDate != null ? "\"" + executionDate + "\"" : null) + "," +
                "\"lastExecutionDate\":" + (lastExecutionDate != null ? "\"" + lastExecutionDate + "\"" : null) + "," +
                "\"activityPostingDate\":" + (activityPostingDate != null ? "\"" + activityPostingDate + "\"" : null) +
                "\"lastActivityPostingDate\":" + (lastActivityPostingDate != null ? "\"" + lastActivityPostingDate + "\"" : null) +
                "\"lastActivityEffectiveDate\":" + (lastActivityEffectiveDate != null ? "\"" + lastActivityEffectiveDate + "\"" : null) +
                "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ExecutionState)) return false;
        ExecutionState that = (ExecutionState) o;
        return Objects.equals(executionDate, that.executionDate) &&
                Objects.equals(lastExecutionDate, that.lastExecutionDate) &&
                Objects.equals(lastActivityPostingDate, that.lastActivityPostingDate) &&
                Objects.equals(activityPostingDate, that.activityPostingDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executionDate, lastExecutionDate, lastActivityPostingDate, activityPostingDate);
    }
}
