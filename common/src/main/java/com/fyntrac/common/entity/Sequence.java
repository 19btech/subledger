package com.fyntrac.common.entity;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "sequences")
public class Sequence {
    @Id
    private String id; // This will hold the identifier for the sequence, e.g., "versionId"
    private int seq;   // This will hold the current sequence value

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

    public int getSeq() {
        return seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }
}
