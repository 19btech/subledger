package com.fyntrac.common.utils;

import com.fyntrac.common.dto.record.Records;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.List;

public class MongoQueryGenerator {

    public static Query generateQuery(List<Records.QueryCriteriaItem> criteriaList) {
        List<Criteria> criteriaListMongo = new ArrayList<>();
        int index=0;
        for (Records.QueryCriteriaItem criteria : criteriaList) {
            Criteria criteriaMongo = null;

            // Handle multiple filters for "contains", "starts with", and "ends with"
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
                    // Generate criteria based on other operators
                    switch (criteria.operator()) {
                        case "equals":
                            criteriaMongo = Criteria.where(criteria.attributeName()).is(criteria.filters().get(0));
                            break;
                        case "not equal":
                            criteriaMongo = Criteria.where(criteria.attributeName()).ne(criteria.filters().get(0));
                            break;
                        case "<":
                            criteriaMongo = Criteria.where(criteria.attributeName()).lt(criteria.filters().get(0));
                            break;
                        case ">":
                            criteriaMongo = Criteria.where(criteria.attributeName()).gt(criteria.filters().get(0));
                            break;
                        case "<=":
                            criteriaMongo = Criteria.where(criteria.attributeName()).lte(criteria.filters().get(0));
                            break;
                        case ">=":
                            criteriaMongo = Criteria.where(criteria.attributeName()).gte(criteria.filters().get(0));
                            break;
                        // Add other cases as needed
                        default:
                            throw new IllegalArgumentException("Invalid operator: " + criteria.operator());
                    }
            }

            // Add criteria to the list
            criteriaListMongo.add(criteriaMongo);

            // If this is not the last criteria, add the logical operator
            if (index < criteriaList.size() - 1) {
                // Handle logical operator
                if (criteria.logicalOperator().equalsIgnoreCase("OR")) {
                    criteriaListMongo.add(new Criteria().orOperator(criteriaMongo));
                } else {
                    if (!criteriaListMongo.isEmpty()) {
                        criteriaListMongo.add(new Criteria().andOperator(criteriaMongo));
                    }
                }
            }
            index++;
        }

        // Combine all criteria into a single query
        Criteria finalCriteria = new Criteria().orOperator(criteriaListMongo.toArray(new Criteria[0]));
        return new Query(finalCriteria);
    }

}