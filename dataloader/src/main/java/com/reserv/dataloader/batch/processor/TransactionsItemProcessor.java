package com.reserv.dataloader.batch.processor;

import com.fyntrac.common.entity.RefDataValidationLog;
import com.fyntrac.common.entity.Transactions;
import com.fyntrac.common.enums.ErrorCode;
import com.reserv.dataloader.batch.exception.ItemValidationException;
import com.reserv.dataloader.validation.TransactionValidator;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemProcessor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionsItemProcessor implements ItemProcessor<Transactions, Transactions> {

    private final TransactionValidator validator;
    private final Set<String> seenTransactionNames = ConcurrentHashMap.newKeySet();

    public TransactionsItemProcessor(TransactionValidator validator) {
        this.validator = validator;
    }

    @BeforeStep
    public void beforeStep(StepExecution stepExecution) {
        this.seenTransactionNames.clear();
    }

    @Override
    public Transactions process(Transactions item) throws Exception {
        List<ItemValidationException.ValidationError> itemLogs = validator.validate(item);

        boolean hasError = itemLogs.stream().anyMatch(log -> "ERROR".equals(log.getSeverity()));
        String name = item.getName();

        if (name != null && !hasError) {
            if (!seenTransactionNames.add(name)) {
                itemLogs.add(new ItemValidationException.ValidationError("NAME", ErrorCode.ERR_DUP_01.name(), "Duplicate transaction name in file: " + name, "ERROR"));
                hasError = true;
            }
        }

        if (hasError) {
            throw new ItemValidationException("Transaction failed validation", itemLogs);
        } else if (!itemLogs.isEmpty()) {
            itemLogs.forEach(log -> {
                System.out.println("Validation Warning: " + log.getMessage());
            });
        }

        return item;
    }
}
