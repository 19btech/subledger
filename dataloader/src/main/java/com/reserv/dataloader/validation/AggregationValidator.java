package com.reserv.dataloader.validation;

import com.fyntrac.common.entity.Aggregation;
import com.fyntrac.common.enums.ErrorCode;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.fyntrac.common.service.TransactionService;
import com.fyntrac.common.service.aggregation.AggregationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

@Component
public class AggregationValidator {

    // Matches alphanumeric and underscore only, NO SPACES or special chars allowed
    private static final Pattern ALPHANUM_UNDERSCORE_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    private final TransactionService transactionService;
    private final AggregationService aggregationService;

    @Autowired
    public AggregationValidator(TransactionService transactionService, AggregationService aggregationService) {
        this.transactionService = transactionService;
        this.aggregationService = aggregationService;
    }

    /**
     * Single-item validation overload for direct real-time REST controller invocation.
     * Guarantees fast, specific multi-tenant safe DB checks.
     */
    public List<ItemValidationException.ValidationError> validate(Aggregation item) {
        List<ItemValidationException.ValidationError> errors = new ArrayList<>();

        String txName = item.getTransactionName();
        String metric = item.getMetricName();

        // 1. Validate transactionName structure and constraints
        if (txName == null || txName.trim().isEmpty()) {
            errors.add(new ItemValidationException.ValidationError(
                    "TRANSACTIONNAME", 
                    ErrorCode.ERR_REQ_01.name(), 
                    "Transaction name is required and cannot be empty.", 
                    "ERROR"
            ));
        } else {
            if (!txName.equals(txName.trim())) {
                errors.add(new ItemValidationException.ValidationError(
                        "TRANSACTIONNAME", 
                        ErrorCode.ERR_SPC_02.name(), 
                        "Transaction name has leading or trailing spaces.", 
                        "ERROR"
                ));
            }

            // Single record validation: check specific transaction configuration in physical tenant context
            try {
                if (transactionService.getTransaction(txName.trim()) == null) {
                    errors.add(new ItemValidationException.ValidationError(
                            "TRANSACTIONNAME", 
                            ErrorCode.ERR_REF_01.name(), 
                            "Transaction name does not exist in transaction reference data.", 
                            "ERROR"
                    ));
                }
            } catch (Exception e) {
                // Best effort
            }
        }

        // 2. Validate metric structure and constraints
        if (metric == null || metric.trim().isEmpty()) {
            errors.add(new ItemValidationException.ValidationError(
                    "METRICNAME", 
                    ErrorCode.ERR_REQ_01.name(), 
                    "Metric name is required and cannot be empty.", 
                    "ERROR"
            ));
        } else {
            if (metric.contains(" ")) {
                errors.add(new ItemValidationException.ValidationError(
                        "METRICNAME", 
                        ErrorCode.ERR_SPC_01.name(), 
                        "Metric contains spaces.", 
                        "ERROR"
                ));
            }

            String trimmedMetric = metric.trim();
            if (!ALPHANUM_UNDERSCORE_PATTERN.matcher(trimmedMetric).matches()) {
                errors.add(new ItemValidationException.ValidationError(
                        "METRICNAME", 
                        ErrorCode.ERR_FMT_01.name(), 
                        "Metric contains special characters.", 
                        "ERROR"
                ));
            }

            // Single record validation: direct multi-tenant safe existence check to prevent duplicates
            try {
                String transactionName = txName != null ? txName.trim() : "";
                List<Aggregation> existingMetrics = aggregationService.getMetrics(transactionName);
                boolean exists = existingMetrics.stream()
                        .anyMatch(agg -> trimmedMetric.equalsIgnoreCase(agg.getMetricName()));
                if (exists) {
                    errors.add(new ItemValidationException.ValidationError(
                            "METRICNAME", 
                            ErrorCode.ERR_DUP_01.name(), 
                            "Metric name '" + trimmedMetric + "' with transaction name '" + transactionName + "' already exists in database.", 
                            "ERROR"
                    ));
                }
            } catch (Exception e) {
                // Best effort
            }
        }

        return errors;
    }

    /**
     * Batch-optimized validation routine using preloaded Set caches to guarantee O(1) lookup speed.
     */
    public List<ItemValidationException.ValidationError> validate(
            Aggregation item, 
            Set<String> validTransactionNames, 
            Set<String> existingMetricTransactionKeys) {
        List<ItemValidationException.ValidationError> errors = new ArrayList<>();

        String txName = item.getTransactionName();
        String metric = item.getMetricName();

        // 1. Validate transactionName
        if (txName == null || txName.trim().isEmpty()) {
            errors.add(new ItemValidationException.ValidationError(
                    "TRANSACTIONNAME", 
                    ErrorCode.ERR_REQ_01.name(), // MANDATORY_FIELD
                    "Transaction name is required and cannot be empty.", 
                    "ERROR"
            ));
        } else {
            // Check for leading or trailing spaces
            if (!txName.equals(txName.trim())) {
                errors.add(new ItemValidationException.ValidationError(
                        "TRANSACTIONNAME", 
                        ErrorCode.ERR_SPC_02.name(), // TRIM_WHITESPACE
                        "Transaction name has leading or trailing spaces.", 
                        "ERROR"
                ));
            }

            // Check if exists in database preloaded Set (case-insensitive or consistent mapping)
            String trimmedTxName = txName.trim().toUpperCase();
            if (!validTransactionNames.contains(trimmedTxName)) {
                errors.add(new ItemValidationException.ValidationError(
                        "TRANSACTIONNAME", 
                        ErrorCode.ERR_REF_01.name(), // REF_NOT_FOUND
                        "Transaction name does not exist in transaction reference data.", 
                        "ERROR"
                ));
            }
        }

        // 2. Validate metric
        if (metric == null || metric.trim().isEmpty()) {
            errors.add(new ItemValidationException.ValidationError(
                    "METRICNAME", 
                    ErrorCode.ERR_REQ_01.name(), // MANDATORY_FIELD
                    "Metric name is required and cannot be empty.", 
                    "ERROR"
            ));
        } else {
            // Check for spaces
            if (metric.contains(" ")) {
                errors.add(new ItemValidationException.ValidationError(
                        "METRICNAME", 
                        ErrorCode.ERR_SPC_01.name(), // NO_WHITESPACE
                        "Metric contains spaces.", 
                        "ERROR"
                ));
            }

            // Check for special characters (not alphanum or underscore)
            // Using trim to only check content
            String trimmedMetric = metric.trim();
            if (!ALPHANUM_UNDERSCORE_PATTERN.matcher(trimmedMetric).matches()) {
                errors.add(new ItemValidationException.ValidationError(
                        "METRICNAME", 
                        ErrorCode.ERR_FMT_01.name(), // ALPHANUM_UNDERSCORE
                        "Metric contains special characters.", 
                        "ERROR"
                ));
            }

            // Check if metric and transaction combination already exists in the database
            String upperMetric = trimmedMetric.toUpperCase();
            String upperTxName = txName != null ? txName.trim().toUpperCase() : "";
            String compositeKey = upperMetric + ":" + upperTxName;
            if (existingMetricTransactionKeys.contains(compositeKey)) {
                errors.add(new ItemValidationException.ValidationError(
                        "METRICNAME",
                        ErrorCode.ERR_DUP_01.name(), // Duplicate error
                        "Metric name '" + trimmedMetric + "' with transaction name '" + (txName != null ? txName.trim() : "") + "' already exists in database.",
                        "ERROR"
                ));
            }
        }

        return errors;
    }
}
