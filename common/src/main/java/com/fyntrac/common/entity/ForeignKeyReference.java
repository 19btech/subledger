package com.fyntrac.common.entity;


import javax.validation.constraints.NotBlank;

public class ForeignKeyReference {

    @NotBlank(message = "Reference table ID is required")
    private String referenceTableId;

    @NotBlank(message = "Reference table name is required")
    private String referenceTableName;

    @NotBlank(message = "Reference column is required")
    private String referenceColumn;

    @NotBlank(message = "Foreign key column is required")
    private String foreignKeyColumn;

    // Constructors
    public ForeignKeyReference() {}

    public ForeignKeyReference(String referenceTableId, String referenceTableName,
                               String referenceColumn, String foreignKeyColumn) {
        this.referenceTableId = referenceTableId;
        this.referenceTableName = referenceTableName;
        this.referenceColumn = referenceColumn;
        this.foreignKeyColumn = foreignKeyColumn;
    }

    // Getters and Setters
    public String getReferenceTableId() { return referenceTableId; }
    public void setReferenceTableId(String referenceTableId) { this.referenceTableId = referenceTableId; }

    public String getReferenceTableName() { return referenceTableName; }
    public void setReferenceTableName(String referenceTableName) { this.referenceTableName = referenceTableName; }

    public String getReferenceColumn() { return referenceColumn; }
    public void setReferenceColumn(String referenceColumn) { this.referenceColumn = referenceColumn; }

    public String getForeignKeyColumn() { return foreignKeyColumn; }
    public void setForeignKeyColumn(String foreignKeyColumn) { this.foreignKeyColumn = foreignKeyColumn; }
}
