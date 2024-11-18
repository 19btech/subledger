package com.fyntrac.common.utils;

public class StringUtil {

    public static int parseBoolean(String value) {
        return ("true".equalsIgnoreCase(value) ||
                "1".equals(value) ||
                "yes".equalsIgnoreCase(value) ||
                "y".equalsIgnoreCase(value)) ? 1 : 0;
    }

    public static String convertToUpperCaseAndRemoveSpaces(String input) {
        if (input == null) {
            return null; // Handle null input gracefully
        }
        // Convert to uppercase and remove all whitespace
        return input.toUpperCase().replaceAll("\\s+", "");
    }

}
