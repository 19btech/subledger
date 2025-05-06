package com.fyntrac.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "Transactions")
public class Transactions {
    @Id
    private String id;
    @Indexed(unique=true)
    private String name;
    private int exclusive;
    private int isGL;
    private int isReplayable;

    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"id\":\"").append(id).append("\",");
        json.append("\"name\":\"").append(name).append("\",");
        json.append("\"exclusive\":").append(exclusive).append(",");
        json.append("\"isGL\":").append(isGL);
        json.append("\"isReplayable\":").append(isReplayable);
        json.append("}");
        return json.toString();
    }

}
