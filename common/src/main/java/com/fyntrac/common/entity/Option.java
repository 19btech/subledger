package com.fyntrac.common.entity;

import com.fyntrac.common.utils.StringUtil;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Option {

    private String label;
    private String value;

    // Constructors
    public Option() {}

    public Option(String label, String value) {
        this.label = label;
        this.value = value;
    }


    // JSON-compatible toString method (manual implementation without Jackson)
    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        // Add fields
        StringUtil.addField(json, "label", label);
        StringUtil.addField(json, "value", value);
        json.append("}");
        return json.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Option option = (Option) o;
        return java.util.Objects.equals(value, option.value);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(value);
    }
}