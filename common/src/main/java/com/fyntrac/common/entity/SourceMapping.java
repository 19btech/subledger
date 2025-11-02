package com.fyntrac.common.entity;

import com.fyntrac.common.enums.FieldType;
import com.fyntrac.common.enums.SourceTable;
import com.fyntrac.common.utils.StringUtil;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;

@Data
@Builder
public class SourceMapping {

    @Field("sourceTable")
    private SourceTable sourceTable;

    @Field("sourceColumns")
    private List<Option> sourceColumns;

    @Field("versionType")
    private List<Option> versionType;

    @Field("dataMapping")
    private List<Option> dataMapping;

    @Field("fieldType")
    private FieldType fieldType;

    // Constructors
    public SourceMapping() {}

    public SourceMapping(SourceTable sourceTable, List<Option> sourceColumns,
                         List<Option> versionType, List<Option> dataMapping, FieldType fieldType) {
        this.sourceTable = sourceTable;
        this.sourceColumns = sourceColumns;
        this.versionType = versionType;
        this.dataMapping = dataMapping;
        this.fieldType = fieldType;
    }


    // JSON-compatible toString method (manual implementation without Jackson)
    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        // Add fields
        StringUtil.addField(json, "sourceTable", sourceTable != null ? sourceTable.toString() : null);
        StringUtil.addListField(json, "sourceColumns", sourceColumns);
        StringUtil.addListField(json, "versionType", versionType);
        StringUtil.addListField(json, "dataMapping", dataMapping);
        StringUtil.addField(json, "fieldType", fieldType);
        json.append("}");
        return json.toString();
    }

}
