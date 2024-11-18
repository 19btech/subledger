package com.fyntrac.common.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;

@Document(collection = "sequences")
public class Sequence implements Serializable {
    @Serial
    private static final long serialVersionUID = -134297821849408526L;
    @Id
    private String id; // This will hold the identifier for the sequence, e.g., "versionId"
    private long seq;   // This will hold the current sequence value

    // Constructors
    public Sequence() {}

    public Sequence(String id, int seq) {
        this.id = id;
        this.seq = seq;
    }

    // Getters and Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }
}
