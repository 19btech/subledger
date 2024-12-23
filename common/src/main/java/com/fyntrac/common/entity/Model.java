package com.fyntrac.common.entity;

import com.fyntrac.common.enums.ModelStatus;
import com.fyntrac.common.enums.UploadStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.index.Indexed;
import java.io.Serial;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
@Document(collection = "Models")
@Data
public class Model implements Serializable {
    @Serial
    private static final long serialVersionUID = -2312211407807116196L;
    @Id
    private String id;
    private String orderId;
    @Indexed(unique = true) // This line adds a unique index on modelName
    private String modelName;
    private Date uploadDate;
    private UploadStatus uploadStatus;
    private ModelStatus modelStatus;
    private String uploadedBy;
    private int isDeleted;
    private Date lastModifiedDate;
    private String modifiedBy;
    private ModelConfig modelConfig;
    private String modelFileId;

    // toString Method
    // Manual JSON-Compatible toString Method
    @Override
    public String toString() {
        StringBuilder json = new StringBuilder("{");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        json.append("id").append(":").append("\"").append(id).append("\"").append(",");
        json.append("orderId").append(":").append("\"").append(orderId).append("\"").append(",");
        json.append("modelName").append(":").append("\"").append(modelName).append("\"").append(",");
        json.append("uploadDate").append(":").append("\"").append(uploadDate != null ? dateFormat.format(uploadDate) : null).append("\"").append(",");
        json.append("uploadStatus").append(":").append("\"").append(uploadStatus != null ? uploadStatus.name() : null).append("\"").append(",");
        json.append("modelStatus").append(":").append("\"").append(modelStatus != null ? modelStatus.name() : null).append("\"").append(",");
        json.append("uploadedBy").append(":").append("\"").append(uploadedBy).append("\"").append(",");
        json.append("isDeleted").append(":").append(isDeleted).append(",");
        json.append("lastModifiedDate").append(":").append("\"").append(lastModifiedDate != null ? dateFormat.format(lastModifiedDate) : null).append("\"").append(",");
        json.append("modifiedBy").append(":").append("\"").append(modifiedBy).append("\"").append(",");
        json.append("modelFileId").append(":").append("\"").append(modelFileId).append("\"").append(",");
        json.append("}");
        return json.toString();
    }


    // equals Method
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Model model = (Model) o;
        return isDeleted == model.isDeleted &&
                Objects.equals(id, model.id) &&
                Objects.equals(orderId, model.orderId) &&
                Objects.equals(modelName, model.modelName) &&
                Objects.equals(uploadDate, model.uploadDate) &&
                Objects.equals(uploadStatus, model.uploadStatus) &&
                Objects.equals(modelStatus, model.modelStatus) &&
                Objects.equals(uploadedBy, model.uploadedBy) &&
                Objects.equals(lastModifiedDate, model.lastModifiedDate) &&
                Objects.equals(modifiedBy, model.modifiedBy) &&
                Objects.equals(modelConfig, model.modelConfig) &&
                Objects.equals(modelFileId, model.modelFileId);
    }

    // hashCode Method
    @Override
    public int hashCode() {
        return Objects.hash(id, orderId, modelName, uploadDate, uploadStatus, modelStatus,
                uploadedBy, isDeleted, lastModifiedDate, modifiedBy, modelConfig, modelFileId);
    }
}
