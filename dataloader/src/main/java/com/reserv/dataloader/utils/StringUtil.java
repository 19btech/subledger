package com.reserv.dataloader.utils;

public class StringUtil {

    public static int parseBoolean(String value) {
        return ("true".equalsIgnoreCase(value) ||
                "1".equals(value) ||
                "yes".equalsIgnoreCase(value) ||
                "y".equalsIgnoreCase(value)) ? 1 : 0;
    }

}
