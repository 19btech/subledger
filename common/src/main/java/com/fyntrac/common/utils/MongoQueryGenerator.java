package com.fyntrac.common.utils;

import com.fyntrac.common.dto.record.Records;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;

public class MongoQueryGenerator {

    public static Query generateQuery(List<Records.QueryCriteriaItem> criteriaList) {
        List<Criteria> criteriaListMongo = new ArrayList<>();

        for (Records.QueryCriteriaItem criteria : criteriaList) {
            Criteria criteriaMongo = null;

            // Helper to parse numeric if possible
            Object parsedValue = parseIfNumber(criteria.filters().get(0));

            switch (criteria.operator()) {
                case "contains":
                    List<Criteria> containsCriteria = new ArrayList<>();
                    for (String filter : criteria.filters()) {
                        containsCriteria.add(Criteria.where(criteria.attributeName()).regex(filter, "i"));
                    }
                    criteriaMongo = new Criteria().orOperator(containsCriteria.toArray(new Criteria[0]));
                    break;

                case "starts with":
                    List<Criteria> startsWithCriteria = new ArrayList<>();
                    for (String filter : criteria.filters()) {
                        startsWithCriteria.add(Criteria.where(criteria.attributeName()).regex("^" + filter, "i"));
                    }
                    criteriaMongo = new Criteria().orOperator(startsWithCriteria.toArray(new Criteria[0]));
                    break;

                case "ends with":
                    List<Criteria> endsWithCriteria = new ArrayList<>();
                    for (String filter : criteria.filters()) {
                        endsWithCriteria.add(Criteria.where(criteria.attributeName()).regex(filter + "$", "i"));
                    }
                    criteriaMongo = new Criteria().orOperator(endsWithCriteria.toArray(new Criteria[0]));
                    break;

                default:
                    switch (criteria.operator()) {
                        case "equals", "==":
                            criteriaMongo = Criteria.where(criteria.attributeName()).is(parsedValue);
                            break;
                        case "not equal", "!=":
                            criteriaMongo = Criteria.where(criteria.attributeName()).ne(parsedValue);
                            break;
                        case "<":
                            criteriaMongo = Criteria.where(criteria.attributeName()).lt(parsedValue);
                            break;
                        case ">":
                            criteriaMongo = Criteria.where(criteria.attributeName()).gt(parsedValue);
                            break;
                        case "<=":
                            criteriaMongo = Criteria.where(criteria.attributeName()).lte(parsedValue);
                            break;
                        case ">=":
                            criteriaMongo = Criteria.where(criteria.attributeName()).gte(parsedValue);
                            break;
                        default:
                            throw new IllegalArgumentException("Invalid operator: " + criteria.operator());
                    }
            }

            if (criteriaMongo != null) {
                criteriaListMongo.add(criteriaMongo);
            }
        }

        Criteria finalCriteria = null;
        for (int i = 0; i < criteriaListMongo.size(); i++) {
            Criteria currentCriteria = criteriaListMongo.get(i);

            if (finalCriteria == null) {
                finalCriteria = currentCriteria;
            } else {
                String logicalOperator = criteriaList.get(i - 1).logicalOperator();
                if ("OR".equalsIgnoreCase(logicalOperator)) {
                    finalCriteria = new Criteria().orOperator(finalCriteria, currentCriteria);
                } else {
                    finalCriteria = new Criteria().andOperator(finalCriteria, currentCriteria);
                }
            }
        }

        return new Query(finalCriteria);
    }

    /**
     * Parses a string to an Integer or Double if it’s numeric, otherwise returns the original string.
     */
    private static Object parseIfNumber(String value) {
        try {
            if (value.matches("\\d+")) {
                return Long.parseLong(value); // Use Long to handle large numbers
            } else if (value.matches("\\d+\\.\\d+")) {
                return Double.parseDouble(value);
            }
        } catch (NumberFormatException ignored) {}
        return value; // Keep as string if not numeric
    }
}
