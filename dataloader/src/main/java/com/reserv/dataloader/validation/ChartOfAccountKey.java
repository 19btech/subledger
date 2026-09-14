package com.reserv.dataloader.validation;

import com.fyntrac.common.entity.ChartOfAccount;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Builds the logical identity of a ChartOfAccount record for duplicate detection.
 *
 * The uniqueness key is accountNumber + accountName + accountSubtype TOGETHER WITH every
 * custom attribute value. The triple on its own is NOT unique: the same
 * number/name/subtype may legitimately be repeated as long as the attribute values differ,
 * which is how one account maps to several attribute combinations. Only a row matching an
 * existing one on all four parts is a duplicate.
 *
 * Both duplicate-detection paths (the batch upload validator and the single-record REST
 * controller) build the key through this class so they cannot drift apart.
 */
public final class ChartOfAccountKey {

    private ChartOfAccountKey() {
    }

    /** Full composite identity: the triple plus the attribute signature. */
    public static String of(ChartOfAccount account) {
        if (account == null) {
            return "|||";
        }
        return normalize(account.getAccountNumber())
                + "|" + normalize(account.getAccountName())
                + "|" + normalize(account.getAccountSubtype())
                + "|" + attributeSignature(account.getAttributes());
    }

    /**
     * Canonical, comparable rendering of a record's custom attributes.
     *
     * Names are sorted case-insensitively so Map iteration order can't affect the result, and
     * each entry is written as name=value rather than positionally, so two records carrying
     * different attribute *keys* produce different signatures. Blank and absent values are
     * treated as the same thing (both omitted), so an attribute left empty does not make a
     * record look distinct from one where it was never supplied.
     *
     * Deliberately derived from the record itself rather than from the Attributes collection:
     * driving the key off externally loaded metadata means an empty or unavailable metadata
     * list silently collapses every record to the same signature, which would make unrelated
     * rows look like duplicates.
     */
    public static String attributeSignature(Map<String, Object> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return "";
        }
        List<String> names = new ArrayList<>(attributes.keySet());
        names.sort(String.CASE_INSENSITIVE_ORDER);

        StringBuilder sb = new StringBuilder();
        for (String name : names) {
            if (name == null) {
                continue;
            }
            String value = normalize(attributes.get(name));
            if (value.isEmpty()) {
                continue; // absent and blank are the same thing
            }
            sb.append(normalize(name)).append('=').append(value).append("||");
        }
        return sb.toString();
    }

    private static String normalize(Object value) {
        return value == null ? "" : String.valueOf(value).trim().toLowerCase();
    }

    /** Human-readable form of the key, for error messages. */
    public static String describe(ChartOfAccount account) {
        if (account == null) {
            return "";
        }
        String attrs = attributeSignature(account.getAttributes());
        return account.getAccountNumber() + "|" + account.getAccountName() + "|" + account.getAccountSubtype()
                + (attrs.isEmpty() ? "" : "|" + attrs);
    }

    /** Unmodifiable empty attribute map, for callers that need a null-safe default. */
    static Map<String, Object> emptyAttributes() {
        return Collections.emptyMap();
    }
}
