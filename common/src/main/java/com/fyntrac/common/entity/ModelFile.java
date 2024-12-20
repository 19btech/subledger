package com.fyntrac.common.entity;

import lombok.Data;
import org.bson.types.Binary;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serial;
import java.io.Serializable;

@Data
@Document(collection = "ModelFiles")
public class ModelFile implements Serializable {
    @Serial
    private static final long serialVersionUID = -3514082712309166929L;
    @Id
    private String id;

    private String contentType;

    private Binary fileData;

    // Getters & Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { this.contentType = contentType; }
    public Binary getFileData() { return fileData; }
    public void setFileData(Binary fileData) { this.fileData = fileData; }
}