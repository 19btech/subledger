package com.fyntrac.common.enums;

import lombok.Getter;

import java.util.Arrays;

@Getter
public enum Currency {
    PKR("Pakistani Rupee", "PKR", "₨"),
    USD("United States Dollar", "USD", "$"),
    GBP("British Pound Sterling", "GBP", "£"),
    EUR("Euro", "EUR", "€"),
    AED("United Arab Emirates Dirham", "AED", "د.إ"),
    QAR("Qatari Riyal", "QAR", "ر.ق"),
    CNY("Chinese Yuan", "CNY", "¥"),
    SAR("Saudi Riyal", "SAR", "﷼"),
    CAD("Canadian Dollar", "CAD", "$"),
    AUD("Australian Dollar", "AUD", "$"),

    // Extra important currencies
    JPY("Japanese Yen", "JPY", "¥"),
    CHF("Swiss Franc", "CHF", "CHF"),
    INR("Indian Rupee", "INR", "₹"),
    SGD("Singapore Dollar", "SGD", "$"),
    HKD("Hong Kong Dollar", "HKD", "$"),
    ZAR("South African Rand", "ZAR", "R"),
    NZD("New Zealand Dollar", "NZD", "$");

    private final String fullName;
    private final String code;
    private final String symbol;

    Currency(String fullName, String code, String symbol) {
        this.fullName = fullName;
        this.code = code;
        this.symbol = symbol;
    }

    public static Currency fromName(String name) {
        return Arrays.stream(Currency.values())
                .filter(c -> c.name().equalsIgnoreCase(name)
                        || c.getCode().equalsIgnoreCase(name)
                        || c.getFullName().equalsIgnoreCase(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No currency found for: " + name));
    }

    // Get currency by code
    public static Currency fromCode(String code) {
        return Arrays.stream(values())
                .filter(c -> c.code.equalsIgnoreCase(code))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Invalid currency code: " + code));
    }

    @Override
    public String toString() {
        return code + " (" + fullName + ", " + symbol + ")";
    }
}
