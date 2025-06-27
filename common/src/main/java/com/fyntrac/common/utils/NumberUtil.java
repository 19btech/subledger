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
}
