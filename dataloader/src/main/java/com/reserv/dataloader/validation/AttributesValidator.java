package com.reserv.dataloader.validation;

import com.fyntrac.common.entity.Attributes;
import com.fyntrac.common.enums.DataType;
import com.fyntrac.common.enums.ErrorCode;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

@Component
public class AttributesValidator {

    private static final Pattern ALPHANUM_UNDERSCORE_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    public List<ItemValidationException.ValidationError> validate(Attributes item, Set<String> existingAttributeNames) {
        List<ItemValidationException.ValidationError> itemLogs = new ArrayList<>();

        String name = item.getAttributeName();
        String rawDataType = item.getRawDataType();
        String rawNullable = item.getRawNullable();

        int reclassable = item.getIsReclassable();
        int versionable = item.getIsVersionable();
        int isNullable = item.getIsNullable();

        boolean hasError = false;

        // 1. Validate attributeName
        if (name == null || name.trim().isEmpty()) {
            itemLogs.add(createError("ATTRIBUTENAME", ErrorCode.ERR_REQ_01, "Attribute name is required and cannot be empty.", "ERROR"));
            hasError = true;
        } else {
            // Leading or trailing spaces
            if (!name.trim().equals(name)) {
                itemLogs.add(createError("ATTRIBUTENAME", ErrorCode.ERR_SPC_02, "Attribute name has leading or trailing spaces.", "ERROR"));
                hasError = true;
            }
            
            // Contains spaces
            if (name.trim().contains(" ")) {
                itemLogs.add(createError("ATTRIBUTENAME", ErrorCode.ERR_SPC_01, "Attribute name contains spaces.", "ERROR"));
                hasError = true;
            }

            // Contains special characters
            String trimmedName = name.trim();
            if (!hasError && !ALPHANUM_UNDERSCORE_PATTERN.matcher(trimmedName).matches()) {
                itemLogs.add(createError("ATTRIBUTENAME", ErrorCode.ERR_FMT_01, "Attribute name contains special characters.", "ERROR"));
                hasError = true;
            }

            // Exists in DB
            if (!hasError) {
                if (existingAttributeNames.contains(trimmedName.toUpperCase())) {
                    itemLogs.add(createError("ATTRIBUTENAME", ErrorCode.ERR_DUP_01, "Duplicate value: Attribute name already exists in database.", "ERROR"));
                    hasError = true;
                }
            }
        }

        // 2. Validate reclassable (boolean)
        if (reclassable == -2) {
            itemLogs.add(createError("RECLASSABLE", ErrorCode.WRN_DEF_01, "Empty reclassable flag. Defaulting to true (1).", "WARNING"));
            item.setIsReclassable(1);
        } else if (reclassable == -1) {
            itemLogs.add(createError("RECLASSABLE", ErrorCode.ERR_BOOL_01, "Invalid boolean format for reclassable.", "ERROR"));
            hasError = true;
        }

        // 3. Validate versionable (boolean)
        if (versionable == -2) {
            itemLogs.add(createError("VERSIONABLE", ErrorCode.WRN_DEF_01, "Empty versionable flag. Defaulting to true (1).", "WARNING"));
            item.setIsVersionable(1);
        } else if (versionable == -1) {
            itemLogs.add(createError("VERSIONABLE", ErrorCode.ERR_BOOL_01, "Invalid boolean format for versionable.", "ERROR"));
            hasError = true;
        }

        // 4. Cross-Field Rule: reclassable = true (1) AND versionable = false (0) -> ERROR
        // Wait! We need to check updated values (e.g. after defaulting)
        if (!hasError) {
            if (item.getIsReclassable() == 1 && item.getIsVersionable() == 0) {
                itemLogs.add(createError("RECLASSABLE/VERSIONABLE", ErrorCode.ERR_LOGIC_02, "Cross-field validation error: Attribute cannot be reclassable without being versionable.", "ERROR"));
                hasError = true;
            }
        }

        // 5. Validate dataType
        if (rawDataType == null || rawDataType.trim().isEmpty()) {
            itemLogs.add(createError("DATATYPE", ErrorCode.ERR_REQ_01, "Data type is required and cannot be empty.", "ERROR"));
            hasError = true;
        } else {
            String cleanType = rawDataType.trim();
            if (!DataType.isValid(cleanType)) {
                itemLogs.add(createError("DATATYPE", ErrorCode.ERR_LIST_01, "Invalid data type allowed value: " + cleanType, "ERROR"));
                hasError = true;
            } else {
                // If valid, ensure parsed datatype matches
                for (DataType type : DataType.values()) {
                    if (type.getValue().equalsIgnoreCase(cleanType)) {
                        item.setDataType(type);
                        break;
                    }
                }
            }
        }

        // 6. Validate nullable
        if (rawNullable == null || rawNullable.trim().isEmpty()) {
            itemLogs.add(createError("NULLABLE", ErrorCode.ERR_REQ_01, "Nullable is required and cannot be empty.", "ERROR"));
            hasError = true;
        } else {
            String cleanNullable = rawNullable.trim().toLowerCase();
            if ("yes".equals(cleanNullable) || "y".equals(cleanNullable) || "true".equals(cleanNullable) || "1".equals(cleanNullable)) {
                item.setIsNullable(1);
            } else if ("no".equals(cleanNullable) || "n".equals(cleanNullable) || "false".equals(cleanNullable) || "0".equals(cleanNullable)) {
                item.setIsNullable(0);
            } else {
                itemLogs.add(createError("NULLABLE", ErrorCode.ERR_LIST_02, "Invalid nullable Yes/No value: " + rawNullable, "ERROR"));
                hasError = true;
            }
        }

        if (name != null && !hasError) {
            item.setAttributeName(name.trim());
        }

        return itemLogs;
    }

    private ItemValidationException.ValidationError createError(String column, ErrorCode errorCode, String message, String severity) {
        return new ItemValidationException.ValidationError(column, errorCode.name(), message, severity);
    }
}
