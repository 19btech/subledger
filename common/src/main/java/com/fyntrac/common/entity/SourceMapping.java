package com.fyntrac.common.entity;

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

    @Field("source_table")
    private SourceTable sourceTable;

    @Field("source_columns")
    private List<Option> sourceColumns;

    @Field("version_type")
    private List<Option> versionType;

    @Field("data_mapping")
    private List<Option> dataMapping;

    // Constructors
    public SourceMapping() {}

    public SourceMapping(SourceTable sourceTable, List<Option> sourceColumns,
                         List<Option> versionType, List<Option> dataMapping) {
        this.sourceTable = sourceTable;
        this.sourceColumns = sourceColumns;
        this.versionType = versionType;
        this.dataMapping = dataMapping;
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
        json.append("}");
        return json.toString();
    }

}
