package com.fyntrac.common.entity;

import com.fyntrac.common.enums.DataActivityType;
import com.fyntrac.common.enums.DataFileType;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;

@Document(collection = "DataFiles")
@Builder
@Data
public class DataFiles {

    @Id
    private String id;

    private String fileName;
    private String contentType;
    private long size;
    private byte[] content;

    private Instant uploadedAt;

    private DataFileType dataFileType;
    private DataActivityType dataActivityType;

    @CreatedDate
    private Instant createdAt;

    // --- No-args constructor (needed by Mongo) ---
    public DataFiles() {
    }

    // --- Full constructor ---
    public DataFiles(String id,
                     String fileName,
                     String contentType,
                     long size,
                     byte[] content,
                     Instant uploadedAt,
                     DataFileType dataFileType,
                     DataActivityType dataActivityType,
                     Instant createdAt) {
        this.id = id;
        this.fileName = fileName;
        this.contentType = contentType;
        this.size = size;
        this.content = content;
        this.uploadedAt = uploadedAt != null ? uploadedAt : Instant.now();
        this.dataFileType = dataFileType;
        this.dataActivityType = dataActivityType;
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "{" +
                "\"id\":\"" + id + "\"," +
                "\"fileName\":\"" + fileName + "\"," +
                "\"contentType\":\"" + contentType + "\"," +
                "\"size\":" + size + "," +
                "\"uploadedAt\":\"" + (uploadedAt != null ? uploadedAt.toString() : null) + "\"," +
                "\"createdAt\":\"" + (createdAt != null ? createdAt.toString() : null) + "\"," +
                "\"dataFileType\":\"" + (dataFileType != null ? dataFileType.name() : null) + "\"," +
                "\"dataActivityType\":\"" + (dataActivityType != null ? dataActivityType.name() : null) + "\"" +
                "}";
    }

    // --- equals ---
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataFiles)) return false;
        DataFiles that = (DataFiles) o;
        return size == that.size &&
                Objects.equals(id, that.id) &&
                Objects.equals(fileName, that.fileName) &&
                Objects.equals(contentType, that.contentType) &&
                Objects.equals(uploadedAt, that.uploadedAt) &&
                Objects.equals(createdAt, that.createdAt) &&
                dataFileType == that.dataFileType &&
                dataActivityType == that.dataActivityType &&
                Arrays.equals(content, that.content);
    }

    // --- hashCode ---
    @Override
    public int hashCode() {
        int result = Objects.hash(id, fileName, contentType, size, uploadedAt, createdAt, dataFileType, dataActivityType);
        result = 31 * result + Arrays.hashCode(content);
        return result;
    }
}
