package com.fyntrac.common.entity;

import com.fyntrac.common.enums.TriggerType;
import com.fyntrac.common.utils.StringUtil;
import lombok.Builder;
import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Field;

import java.util.List;
@Data
@Builder
public class TriggerSetup {

    @Field("triggerType")
    private TriggerType triggerType;

    @Field("triggerCondition")
    private String triggerCondition;

    @Field("triggerSource")
    private List<Option> triggerSource;

    // Constructors
    public TriggerSetup() {}

    public TriggerSetup(TriggerType triggerType, String triggerCondition, List<Option> triggerSource) {
        this.triggerType = triggerType;
        this.triggerCondition = triggerCondition;
        this.triggerSource = triggerSource;
    }

    // JSON-compatible toString method (manual implementation without Jackson)
    @Override
    public String toString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        // Add fields
        StringUtil.addField(json, "triggerType", triggerType);
        StringUtil.addField(json, "triggerCondition", triggerCondition);
        StringUtil.addListField(json, "triggerSource", triggerSource);
        json.append("}");
        return json.toString();
    }
}
