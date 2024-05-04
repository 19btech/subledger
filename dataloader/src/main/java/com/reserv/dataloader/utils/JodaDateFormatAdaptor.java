package com.reserv.dataloader.utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.ParsePosition;
import java.util.Date;
import java.util.TimeZone;

/**
 * The Date formatter adaptor to the Joda <code>DateTimeFormatter</code>.
 */
public class JodaDateFormatAdaptor extends DateFormat {
    /** The formatter used for parsing/formatting. */
    private final DateTimeFormatter format;

    /** The parsing pattern. */
    private final String pattern;

    public JodaDateFormatAdaptor() {
        this(DateUtil.DATE_FORMAT, TimeZone.getDefault());
    }

    public JodaDateFormatAdaptor(String pattern) {
        this(pattern, TimeZone.getDefault());
    }

    public JodaDateFormatAdaptor(String pattern, TimeZone timezone) {
        // The forPattern method contains a synchronized block in it so
        // we only want to call this once in the constructor here, not
        // every time we want to format or parse with this instance.
        super();
        this.pattern = pattern;

        if (pattern.equals(DateUtil.DATE_FORMAT)) {
            // Optimization to avoid a synchronized block in the
            // forPattern() method.
            this.format = DateUtil.defaultFormat;
        } else {
            this.format = DateTimeFormat.forPattern(pattern);
        }

        if (!TimeZone.getDefault().getID().equals(timezone.getID())) {
            this.format.withZone(DateTimeZone.forTimeZone(timezone));
        }
    }

    /**
     * This method Overrides the format method of the Date Formatter.
     */
    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo,
                               FieldPosition fieldPosition) {
        fieldPosition.setBeginIndex(0);
        fieldPosition.setEndIndex(0);

        DateTimeFormatter format = this.format;
        DateTime jdate = new DateTime(date);

        if (!pattern.equals(DateUtil.DATE_FORMAT_EXTENDED) &&
                (
                        (jdate.getHourOfDay() != 0) || (jdate.getMinuteOfHour() != 0) ||
                                (jdate.getSecondOfMinute() != 0)
                )) {
            format = DateTimeFormat.forPattern(DateUtil.DATE_FORMAT_EXTENDED);

            // Only set zone if its really different since setting the
            // zone is synchronized.
            DateTimeZone myZone = this.format.getZone();
            DateTimeZone newZone = format.getZone();

            if ((myZone != null) &&
                    (newZone != null) &&
                    !newZone.getID().equals(myZone.getID())) {
                format.withZone(myZone);
            }
        }

        // Returning the formatted Date
        return new StringBuffer(format.print(new DateTime(date)));
    }

    /**
     * This method override the parse method of the DateFormat.
     */
    @Override
    public Date parse(String source, ParsePosition pos) {
        // Actual parsing done using the DateTimeFormatter
        Date convertedDate = format.parseDateTime(source).toDate();
        pos.setIndex(source.length());
        pos.setErrorIndex(0);

        return convertedDate;
    }

    /**
     * Creates a copy of this <code>DateFormat</code>. This also
     * clones the format's date format symbols.
     * @return a clone of this <code>DateFormat</code>.
     */
    public Object clone() {
        return super.clone();
    }

    /**
     * Returns the hash code value for this <code>JodaDateFormatAdaptor</code>
     * object.
     * @return the hash code value for this <code>JodaDateFormatAdaptor</code>
     * object.
     */
    public int hashCode() {
        return format.hashCode();
    }

    /**
     * Compares the given object with this <code>DateFormat</code> for
     * equality.
     * @return true if the given object is equal to this <code>DateFormat</code>
     */
    public boolean equals(Object obj) {
        if (obj instanceof JodaDateFormatAdaptor) {
            JodaDateFormatAdaptor that = (JodaDateFormatAdaptor) obj;

            return format.equals(that.format);
        } else {
            return false;
        }
    }
}

