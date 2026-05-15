package com.reserv.dataloader.validation;

import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.enums.ErrorCode;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.fyntrac.common.service.TransactionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

@Component
public class TransactionValidator {

    private static final Pattern ALPHANUM_UNDERSCORE_PATTERN = Pattern.compile("^[a-zA-Z0-9_ ]+$");

    private final TransactionService transactionService;

    @Autowired
    public TransactionValidator(TransactionService transactionService) {
        this.transactionService = transactionService;
    }

    public com.fyntrac.common.service.TransactionService getTransactionService() {
        return this.transactionService;
    }

    public List<ItemValidationException.ValidationError> validate(Transactions item) {
        List<ItemValidationException.ValidationError> itemLogs = new ArrayList<>();

        String name = item.getName();
        int isGL = item.getIsGL();
        int isReplayable = item.getIsReplayable();
        boolean hasError = false;

        // Validate transactionName
        if (name == null || name.trim().isEmpty()) {
            itemLogs.add(createError("NAME", ErrorCode.ERR_REQ_01, "Transaction name cannot be empty.", "ERROR"));
            hasError = true;
        } else {
            if (name.contains(" ")) {
                if (name.trim().equals(name) && !name.contains("  ")) {
                    itemLogs.add(createError("NAME", ErrorCode.ERR_SPC_01, "Transaction name contains spaces.", "ERROR"));
                    hasError = true;
                }
            }
            if (!name.trim().equals(name)) {
                itemLogs.add(createError("NAME", ErrorCode.ERR_SPC_02, "Transaction name has leading/trailing spaces.", "ERROR"));
                hasError = true;
            }

            if (!hasError && !ALPHANUM_UNDERSCORE_PATTERN.matcher(name.trim()).matches()) {
                itemLogs.add(createError("NAME", ErrorCode.ERR_FMT_01, "Transaction name contains special characters.", "ERROR"));
                hasError = true;
            }

            if (!hasError) {
                try {
                    Transactions existing = transactionService.getTransaction(name.trim());
                    if (existing != null) {
                        // Pick object from validation only where ID is NOT equal to request object (Excludes self on update)
                        if (item.getId() == null || !existing.getId().equals(item.getId())) {
                            itemLogs.add(createError("NAME", ErrorCode.ERR_DUP_01, "Duplicate transaction name in db: " + name.trim(), "ERROR"));
                            hasError = true;
                        }
                    }
                } catch (Exception e) {
                    // DB duplicate check is best-effort; skip if tenant context is unavailable
                    // (e.g., during batch processing where in-file dedup is handled separately)
                }
            }
        }

        // Validate journal (isGL)
        if (isGL == -2) {
            itemLogs.add(createError("ISGL", ErrorCode.WRN_DEF_01, "Empty journal flag. Defaulting to true (1).", "WARNING"));
            item.setIsGL(1);
        } else if (isGL == -1) {
            itemLogs.add(createError("ISGL", ErrorCode.ERR_BOOL_01, "Invalid boolean value for journal.", "ERROR"));
            hasError = true;
        }

        // Validate reportable (isReplayable)
        if (isReplayable == -2) {
            itemLogs.add(createError("ISREPLAYABLE", ErrorCode.WRN_DEF_01, "Empty reportable flag. Defaulting to true (1).", "WARNING"));
            item.setIsReplayable(1);
        } else if (isReplayable == -1) {
            itemLogs.add(createError("ISREPLAYABLE", ErrorCode.ERR_BOOL_01, "Invalid boolean value for reportable.", "ERROR"));
            hasError = true;
        }

        // Validate Logic Warning
        if (item.getIsGL() == 0 && item.getIsReplayable() == 0) {
            itemLogs.add(createError("ISGL/ISREPLAYABLE", ErrorCode.WRN_LOGIC_01, "Both journal and reportable are false.", "WARNING"));
        }

        // Validate exclusive
        if (item.getExclusive() == -1) {
            item.setExclusive(0);
        }

        if (name != null && !hasError) {
            item.setName(name.trim());
        }

        return itemLogs;
    }

    public List<ItemValidationException.ValidationError> validate(Transactions item, java.util.Set<String> existingTransactionNames) {
        List<ItemValidationException.ValidationError> itemLogs = new ArrayList<>();

        String name = item.getName();
        int isGL = item.getIsGL();
        int isReplayable = item.getIsReplayable();
        boolean hasError = false;

        // Validate transactionName
        if (name == null || name.trim().isEmpty()) {
            itemLogs.add(createError("NAME", ErrorCode.ERR_REQ_01, "Transaction name cannot be empty.", "ERROR"));
            hasError = true;
        } else {
            if (name.contains(" ")) {
                if (name.trim().equals(name) && !name.contains("  ")) {
                    itemLogs.add(createError("NAME", ErrorCode.ERR_SPC_01, "Transaction name contains spaces.", "ERROR"));
                    hasError = true;
                }
            }
            if (!name.trim().equals(name)) {
                itemLogs.add(createError("NAME", ErrorCode.ERR_SPC_02, "Transaction name has leading/trailing spaces.", "ERROR"));
                hasError = true;
            }

            if (!hasError && !ALPHANUM_UNDERSCORE_PATTERN.matcher(name.trim()).matches()) {
                itemLogs.add(createError("NAME", ErrorCode.ERR_FMT_01, "Transaction name contains special characters.", "ERROR"));
                hasError = true;
            }

            if (!hasError) {
                if (existingTransactionNames.contains(name.trim().toUpperCase())) {
                    itemLogs.add(createError("NAME", ErrorCode.ERR_DUP_01, "Duplicate transaction name in db: " + name.trim(), "ERROR"));
                    hasError = true;
                }
            }
        }

        // Validate journal (isGL)
        if (isGL == -2) {
            itemLogs.add(createError("ISGL", ErrorCode.WRN_DEF_01, "Empty journal flag. Defaulting to true (1).", "WARNING"));
            item.setIsGL(1);
        } else if (isGL == -1) {
            itemLogs.add(createError("ISGL", ErrorCode.ERR_BOOL_01, "Invalid boolean value for journal.", "ERROR"));
            hasError = true;
        }

        // Validate reportable (isReplayable)
        if (isReplayable == -2) {
            itemLogs.add(createError("ISREPLAYABLE", ErrorCode.WRN_DEF_01, "Empty reportable flag. Defaulting to true (1).", "WARNING"));
            item.setIsReplayable(1);
        } else if (isReplayable == -1) {
            itemLogs.add(createError("ISREPLAYABLE", ErrorCode.ERR_BOOL_01, "Invalid boolean value for reportable.", "ERROR"));
            hasError = true;
        }

        // Validate Logic Warning
        if (item.getIsGL() == 0 && item.getIsReplayable() == 0) {
            itemLogs.add(createError("ISGL/ISREPLAYABLE", ErrorCode.WRN_LOGIC_01, "Both journal and reportable are false.", "WARNING"));
        }

        // Validate exclusive
        if (item.getExclusive() == -1) {
            item.setExclusive(0);
        }

        if (name != null && !hasError) {
            item.setName(name.trim());
        }

        return itemLogs;
    }

    private ItemValidationException.ValidationError createError(String column, ErrorCode errorCode, String message, String severity) {
        return new ItemValidationException.ValidationError(column, errorCode.name(), message, severity);
    }
}
