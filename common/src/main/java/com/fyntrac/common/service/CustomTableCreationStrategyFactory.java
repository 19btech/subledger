package com.fyntrac.common.service;

import com.fyntrac.common.entity.CustomTableDefinition;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CustomTableCreationStrategyFactory {

    private final List<CustomTableCreationStrategy> strategies;

    public CustomTableCreationStrategyFactory(List<CustomTableCreationStrategy> strategies) {
        this.strategies = strategies;
    }

    public CustomTableCreationStrategy getStrategy(CustomTableDefinition tableDefinition) {
        return strategies.stream()
                .filter(strategy -> strategy.supports(tableDefinition))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "No table creation strategy found for table type: " + tableDefinition.getTableType()));
    }
}
