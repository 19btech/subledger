package com.fyntrac.common.utils;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DateConverter {

    // Array of possible date formats
    private static final String[] DATE_FORMATS = {
            "yyyy-MM-dd",          // 2023-10-01
            "MM/dd/yyyy",          // 10/01/2023
            "dd-MM-yyyy",          // 01-10-2023
            "yyyy/MM/dd",          // 2023/10/01
            "dd MMM yyyy",         // 01 Oct 2023
            "MMM dd, yyyy",        // Oct 01, 2023
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", // 2023-10-01T10:00:00.000Z
            "yyyy-MM-dd'T'HH:mm:ss", // 2023-10-01T10:00:00
            "yyyy-MM-dd HH:mm:ss", // 2023-10-01 10:00:00
            "MM-dd-yyyy",          // 10-01-2023
            "dd/MM/yyyy",          // 01/10/2023
            "yyyyMMdd",            // 20231001
            // Add more formats as needed
    };

    public static Date convertToDate(String dateString) {
        for (String format : DATE_FORMATS) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(format, Locale.ENGLISH);
                return sdf.parse(dateString);
            } catch (ParseException e) {
                // Ignore and try the next format
            }
        }
        // If no format matched, throw an exception or return null
        throw new IllegalArgumentException("Unparseable date: " + dateString);
    }
}
