package com.fyntrac.common.utils;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;

public class StringUtil {

    public static String toCamelCase(String input) {
        if (input == null || input.isEmpty()) return input;

        String[] parts = input.toLowerCase().split("[\\s_\\-]+"); // splits on space, underscore, dash
        StringBuilder camelCase = new StringBuilder(parts[0]);

        for (int i = 1; i < parts.length; i++) {
            if (!parts[i].isEmpty()) {
                camelCase.append(Character.toUpperCase(parts[i].charAt(0)))
                        .append(parts[i].substring(1));
            }
        }

        return camelCase.toString();
    }

    public static String toTitleCase(String input) {
        if (input == null || input.isBlank()) {
            return input;
        }

        String[] words = input.trim().toLowerCase().split("\\s+");
        StringBuilder titleCase = new StringBuilder();

        for (String word : words) {
            if (!word.isEmpty()) {
                titleCase.append(Character.toUpperCase(word.charAt(0)))
                        .append(word.substring(1))
                        .append(" ");
            }
        }

        return titleCase.toString().trim();
    }

    public static int parseBoolean(String value) {
        return ("true".equalsIgnoreCase(value) ||
                "1".equals(value) ||
                "1.0".equals(value) ||
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

    public static String getStackTrace(Exception e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw); // Print the stack trace to the PrintWriter
        return sw.toString(); // Convert the StringWriter to a string
    }

    public static String getStackTrace(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw); // Print the stack trace to the PrintWriter
        return sw.toString(); // Convert the StringWriter to a string
    }


    public static String toProperString(String input) {
        if (input == null || input.isEmpty()) {
            return ""; // Return an empty string if the input is null or empty
        }

        // Trim leading and trailing spaces
        input = input.trim();

        // Split the string into words
        String[] words = input.split("\\s+");

        // Capitalize the first letter of each word and join them back
        StringBuilder properString = new StringBuilder();
        for (String word : words) {
            if (!word.isEmpty()) {
                // Capitalize the first letter and append the rest of the word
                properString.append(Character.toUpperCase(word.charAt(0)))
                        .append(word.substring(1).toLowerCase())
                        .append(" "); // Add a space after each word
            }
        }

        // Remove the trailing space and return the result
        return properString.toString().trim();
    }

    public static List<String> convertUpperCase(List<String> list) {
        return  list.stream()
                .map(String::toUpperCase) // Convert each string to uppercase
                .collect(Collectors.toList()); // Collect the results into a new list
    }

    public static List<String> removeSpaces(List<String> list) {
        return list.stream()
                .map(s -> s.trim()
                        .replaceAll("[\\u2013\\u2014]", "-") // Replace en/em dashes with hyphen
                        .replaceAll("\\s+", " ")            // Normalize whitespace
                        .toUpperCase())
                .collect(Collectors.toList());
    }

    public static String removeSpaces(String str) {
        return str.replaceAll("[\\u2013\\u2014]", "-") // Replace en/em dashes with hyphen
                        .replaceAll("\\s+", " ")            // Normalize whitespace
                        .toUpperCase();
    }

    public static String generateSHA256Hash(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes());
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    // Helper method to add a field to JSON
    public static void addField(StringBuilder json, String key, Object value) {
        addField(json, key, value, false);
    }
    public static  void addField(StringBuilder json, String key, Object value, boolean isLast) {
        json.append("\"").append(key).append("\":");
        if (value == null) {
            json.append("null");
        } else if (value instanceof String) {
            json.append("\"").append(escapeJsonString((String) value)).append("\"");
        } else if (value instanceof Boolean || value instanceof Number) {
            json.append(value.toString());
        } else {
            // For complex objects, assume they have JSON-compatible toString
            json.append(value.toString());
        }
        if (!isLast) {
            json.append(",");
        }
    }
    // Helper method to add a list field to JSON
    public static  void addListField(StringBuilder json, String key, List<?> list) {
        json.append("\"").append(key).append("\":[");
        if (list != null && !list.isEmpty()) {
            for (int i = 0; i < list.size(); i++) {
                json.append(list.get(i).toString());
                if (i < list.size() - 1) {
                    json.append(",");
                }
            }
        }
        json.append("],");
    }
    // Helper method to escape JSON strings
    public static  String escapeJsonString(String str) {
        return str.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }
}
