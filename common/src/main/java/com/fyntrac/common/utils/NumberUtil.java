package com.fyntrac.common.utils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParsePosition;

public class NumberUtil {
    private static final int SCALE = 4;
    public static double getDouble(double activity) {
        DecimalFormat df = new DecimalFormat("#.####");
       return  Double.parseDouble(df.format(activity));
    }

    public static boolean isValidNumber(String input) {
        if (input == null || input.isBlank()) {
            return false;
        }

        NumberFormat format = NumberFormat.getInstance();
        ParsePosition pos = new ParsePosition(0);
        format.setGroupingUsed(true); // Allows commas in numbers like 1,000.5

        format.parse(input, pos);

        // Check if entire string was consumed during parsing
        return pos.getIndex() == input.length();
    }

    /**
     * Normalize a BigDecimal to 4 decimal places using HALF_UP rounding.
     *
     * @param value The input value (nullable)
     * @return Scaled BigDecimal or null if input is null
     */
    public static BigDecimal getNumber(BigDecimal value) {
        return value != null ? value.setScale(SCALE, RoundingMode.HALF_UP) : null;
    }

    /**
     * Strips a redundant trailing ".0" from whole-number strings, e.g. "1000.0" -> "1000".
     * <p>
     * Numeric spreadsheet cells (see ExcelUtil#getNumericValue) are read as Doubles, so a value
     * typed as "1000" in a sheet arrives downstream as "1000.0". This normalizes that back for
     * fields such as account numbers, while leaving genuinely alphanumeric values
     * (e.g. "AC-1000") and true decimals (e.g. "1000.5") untouched.
     *
     * @param value The input value (nullable)
     * @return the normalized string, or the trimmed original if it isn't a plain whole number
     */
    public static String normalizeWholeNumberString(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        try {
            double parsed = Double.parseDouble(trimmed);
            if (!Double.isInfinite(parsed) && !Double.isNaN(parsed) && parsed == Math.floor(parsed)) {
                return String.valueOf((long) parsed);
            }
        } catch (NumberFormatException e) {
            // Not a plain number (e.g. alphanumeric account number) - leave as-is.
        }
        return trimmed;
    }
}
