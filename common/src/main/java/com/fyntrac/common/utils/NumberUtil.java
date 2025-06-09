package com.fyntrac.common.utils;

import java.text.DecimalFormat;

public class NumberUtil {

    public static double getDouble(double activity) {
        DecimalFormat df = new DecimalFormat("#.####");
       return  Double.parseDouble(df.format(activity));
    }
}
