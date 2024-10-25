package com.reserv.dataloader.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import org.apache.commons.lang.math.Range;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.Interval;
import org.joda.time.MutableDateTime;
import org.joda.time.Years;
import org.joda.time.chrono.GJChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import java.text.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * This is a helper class for collecting date and time related methods.
 * This class extends {@link org.apache.commons.lang.time.DateUtils}
 * so that clients can use this class to insulate themselves from
 * the commons-lang package.
 */
public class DateUtil extends org.apache.commons.lang.time.DateUtils {


    private static Logger logger = LogManager.getLogger(DateUtil.class);


    /** The canonical date format used by the application. */
    public static final String DATE_FORMAT = "yyyy/MM/dd";

    /** The canonical extended date format used by the application. */
    public static final String DATE_FORMAT_EXTENDED = "MM/dd/yyyy HH:mm:ss";

    /** The canonical extended date format used by the application. */
    public static final String DATE_TIME_FORMAT = "MM/dd/yyyy hh:mm a";

    public static final String MYSQL_DATE_FORMAT = "MM/dd/yyyy";

    /** The canonical extended date format used by the application. */
    public static final String DATE_FORMAT_EXTENDED_WITH_TIMEZONE =
            "MM/dd/yyyy hh:mm a zzz";

    /** This factory associates a time zone with each thread. */
    public static final TimeZoneThreadLocal timeZoneFactory =
            new TimeZoneThreadLocal();

    /** The default date formatter (uses the default timezone). */
    public static final DateTimeFormatter defaultFormat =
            DateTimeFormat.forPattern(DATE_FORMAT);

    /** The default date formatter (uses the default timezone). */
    public static final DateTimeFormatter defaultTimeStampFormat =
            DateTimeFormat.forPattern(DATE_FORMAT_EXTENDED);

    /** The default time zone id. */
    private static final String defaultTimeZoneId =
            TimeZone.getDefault().getID();

    /** The default calendar instance, used to generate clones. */
    private static final Calendar defaultCalendar = Calendar.getInstance();


    /**
     * This method gets the time zone associated with the calling thread.
     * @return The current TimeZone
     */
    public static TimeZone getTimeZone() {
        return timeZoneFactory.getTimeZone();
    }

    /**
     * Formats the specified date in the form MM/dd/yyyy.
     * Also takes into account GJ Chronology Basis for dates older than year 01/01/1583
     * @param date The date to format.
     * @return The formatted date in the form MM/dd/yyyy.
     */
    public static String format(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int year = cal.get(Calendar.YEAR);

        if (year > 1583) {
            return defaultFormat.print(new DateTime(cal.getTime()));
        } else {
            cal.add(Calendar.HOUR_OF_DAY, 1);

            return defaultFormat.print(
                    new DateTime(cal.getTime(), GJChronology.getInstance()));
        }
    }

    /**
     * Formats the specified date in the form MM/dd/yyyy.
     * @param date The date to format.
     * @return The formatted date in the form MM/dd/yyyy.
     */
    public static String format(Date date, String pattern) {
        DateTimeFormatter format =
                pattern.equals(DATE_FORMAT) ? defaultFormat
                        : DateTimeFormat.forPattern(pattern);

        return format.print(new DateTime(date));
    }

    /**
     * Create a new DateFormat object that is localized to the
     * time zone associated with the thread.
     *
     * @return A new DateFormat with the given time zone.
     */
    public static DateFormat createDateFormat() {
        return createDateFormat(timeZoneFactory.getTimeZone(), DATE_FORMAT);
    }

    /**
     * Create a new DateFormat object that is localized to the
     * given time zone.
     *
     * @param tz The time zone of the date format to create
     * @return A new DateFormat with the given time zone.
     */
    public static DateFormat createDateFormat(TimeZone tz) {
        return createDateFormat(tz, DATE_FORMAT);
    }

    /**
     * Create a new DateFormat object that is localized to the
     * time zone associated with the thread.
     *
     * @param  Input date format for creating date format object
     * @return A new DateFormat with the given time zone.
     */
    public static DateFormat createDateFormat(String dateFormat) {
        return createDateFormat(timeZoneFactory.getTimeZone(), dateFormat);
    }

    /**
     * Create a new DateFormat object that is localized to the
     * given time zone.(Date) value
     *
     * @param tz The time zone of the date format to create
     * @param dateFormat Input format for creating date format
     * @return A new DateFormat with the given time zone.
     */
    public static DateFormat createDateFormat(TimeZone tz, String dateFormat) {
        return new JodaDateFormatAdaptor(dateFormat, tz);
    }

    /**
     * Get the difference between two dates in months, taking into account
     * only their month and year fields.
     *
     * @param end   The date start subtract end
     * @param start The date start be subtracted
     * @return The difference in months of end - start.
     */
    public static int getDateDiffMonths(Date end, Date start) {
        return getDateDiffMonths(end, start, timeZoneFactory.getTimeZone());
    }

    /**
     * Get the difference between two dates in months, taking into account
     * only their month and year fields.
     *
     * @param end   The date start subtract end
     * @param start The date start be subtracted
     * @param tz    The time zone in which the calculation is start be made.
     * @return The difference in months of end - start.
     */
    public static int getDateDiffMonths(Date end, Date start, TimeZone tz) {
        DateTime dt1 = new DateTime(start.getTime());
        DateTime dt2 = new DateTime(end.getTime());

        int numOfMonths =
                ((dt2.getYear() * 12) + dt2.getMonthOfYear()) -
                        ((dt1.getYear() * 12) + dt1.getMonthOfYear());

        return numOfMonths;
    }

    /**
     * Get the difference between two dates in months.
     * Diff between 1/3 and 12/27 is 12 months because 1/3 to 12/3 is 11 months
     * and 12/4 to 12/27 is considered to another period for amortization.
     *
     * @param end   The date start subtract end
     * @param start The date start be subtracted
     * @param tz    The time zone in which the calculation is start be made.
     * @return The difference in months of end - start.
     */
    public static int getDateDiffMonthsRoundUp(Date end,
                                               Date start,
                                               TimeZone tz) {
        return getDateDiffMonthsRoundUp(end, start);
    }

    /**
     * Get the difference between two dates in months.
     * Diff between 1/3 and 12/27 is 12 months because 1/3 to 12/3 is 11 months
     * and 12/4 to 12/27 is considered to another period for amortization.
     *
     * @param end   The date start subtract end
     * @param start The date start be subtracted
     * @return The difference in months of end - start.
     */
    public static int getDateDiffMonthsRoundUp(Date end, Date start) {
        DateTime dt1 = new DateTime(start.getTime());
        DateTime dt2 = new DateTime(end.getTime());

        int numOfMonths =
                ((dt2.getYear() * 12) + dt2.getMonthOfYear()) -
                        ((dt1.getYear() * 12) + dt1.getMonthOfYear());

        if (dt2.getDayOfMonth() > dt1.getDayOfMonth()) {
            numOfMonths++;
        }

        return numOfMonths;
    }

    /**
     * Compare two Date objects based on their date fields and ignoring any
     * time component.
     *
     * @param date1 The first date to compare
     * @param date2 The second date to compare
     * @param tz    The time zone used to interpret the dates
     * @return An integer less than, equal to, or greater than 0 if date1 is
     *         less than, equal to, or greater than date2 respectively.
     */
    public static int compareDays(Date date1, Date date2, TimeZone tz) {
        return getDateDiffDays(date1, date2, tz);
    }

    /**
     * Compare two Date objects based on their date fields and ignoring any
     * time component.  This method uses the TimeZone associated with the
     * calling thread.
     *
     * @param date1 The first date to compare
     * @param date2 The second date to compare
     * @return An integer less than, equal to, or greater than 0 if date1 is
     *         less than, equal to, or greater than date2 respectively.
     */
    public static int compareDays(Date date1, Date date2) {
        return getDateDiffDays(date1, date2, timeZoneFactory.getTimeZone());
    }

    /**
     * Compare two Calendar objects based on their date fields and ignoring any
     * time component.
     *
     * @param cal1 The first date to compare
     * @param cal2 The second date to compare
     * @return An integer less than, equal to, or greater than 0 if cal1 is
     *         less than, equal to, or greater than cal2 respectively.
     */
    public static int compareDays(Calendar cal1, Calendar cal2) {
        return getDateDiffDays(cal1, cal2);
    }

    /**
     * Compare two dates down to second precision, but ignoring milliseconds.
     *
     * @param date1 The first date to compare.
     * @param date2 The second date to compare.
     * @return <code>true</code> If the two input dates are the same down
     *         to the second level of precision.
     * @throws NullPointerException If either of the input dates are
     *                              <code>null</code>.
     */
    public static boolean isSameSecond(Date date1, Date date2) {
        return (date1.getTime() / 1000) == (date2.getTime() / 1000);
    }

    /**
     * Get current date in MM/dd/YYYY format.
     *
     * @return <code>String</code> string representation of current date.
     */
    public static String getCurrentDate() {
        DateFormat formatter = new JodaDateFormatAdaptor(DATE_FORMAT);

        return formatter.format(new Date());
    }

    /**
     * The start date of the range
     *
     * @param range a LongRange object
     * @return the Date where the range starts
     */
    public static Date rangeToStartDate(Range range) {
        return new Date(range.getMinimumLong());
    }

    /**
     * The end date for the range
     *
     * @param range a LongRange
     * @return the end java.util.Date for the range
     */
    public static Date rangeToEndDate(Range range) {
        return new Date(range.getMaximumLong());
    }

    /**
     * This method adds (or subtracts) months from a date, but keeps
     * the day of the {@link Calendar#DAY_OF_MONTH} field constant
     * while accounting for differences in the number of days in
     * each month.  For example, if the desired dayOfMonth parameter
     * is 31 and the month is changed to April using this method,
     * then the resulting date will be April 30 since April only
     * has 30 days.
     *
     * @param date       The date to modify
     * @param months     The number of months to add.  This may be negative
     *                   to move the date backwards.
     * @param dayOfMonth The day of the month of the resulting date.
     *                   If this is greater than the number of days in the month,
     *                   the resulting date will be the last valid day of the month.
     * @return A new date reflecting the change.
     */
    public static Date addMonths(Date dt, int months, int dayOfMonth) {
        if (dt == null) {
            return null;
        }

        DateTime result = new DateTime(dt).monthOfYear().addToCopy(months);
        int maxDate = result.dayOfMonth().getMaximumValue();

        return result
                .withDayOfMonth((maxDate < dayOfMonth) ? maxDate : dayOfMonth)
                .toDate();
    }

    public static Date addMonths(Date dt, int months) {
        if (dt == null) {
            return null;
        }

        return new DateTime(dt).monthOfYear().addToCopy(months).toDate();
    }

    /**
     * This method is the same as the {@link #addMonths(Date,int,int)}
     * method except it takes a {@link Calendar} instance representing
     * the date to be changed.  Note that this method actually modifies
     * the date in the input calendar as well as returning the updated
     * date.
     *
     * @param cal        The calendar that is set to the date to be modified.
     *                   This object will be modified to reflect the changed date.
     * @param months     The number of months to add.  This may be negative
     *                   to move the date backwards.
     * @param dayOfMonth The day of the month of the resulting date.
     *                   If this is greater than the number of days in the month,
     *                   the resulting date will be the last valid day of the month.
     * @return A new date reflecting the change.
     */
    public static Date addMonths(Calendar cal, int months, int dayOfMonth) {
        cal.add(Calendar.MONTH, months);

        if (cal.get(Calendar.DAY_OF_MONTH) != dayOfMonth) {
            // Adjust the day
            int maxDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
            cal.set(Calendar.DAY_OF_MONTH, Math.min(dayOfMonth, maxDay));
        }

        return cal.getTime();
    }

    /**
     * Get a Calendar instance with the time zone set to the
     * time zone associated with the calling thread.
     *
     * @return A Calendar instance with the time zone set.
     */
    public static Calendar calendar() {
        return calendar(timeZoneFactory.getTimeZone());
    }

    /**
     * Get a Calendar instance with the time zone set to the
     * time zone associated with the calling thread.
     * @param tz The TimeZone to grab a calendar instance for.
     * @return A Calendar instance with the time zone set.
     */
    public static Calendar calendar(TimeZone tz) {
        if ((tz != null) && !tz.getID().equals(defaultTimeZoneId)) {
            return Calendar.getInstance(tz);
        } else {
            Calendar cal = (Calendar) defaultCalendar.clone();
            cal.setTime(new Date());

            return cal;
        }
    }

    /**
     * Get a Calendar instance with the time zone set to the
     * time zone associated with the calling thread, and the
     * time in the calendar set to the input date.
     *
     * @param date that should be set in the
     *             resulting calendar.
     * @return A Calendar instance with the time zone set.
     */
    public static Calendar calendar(Date date) {
        Calendar cal = calendar();
        cal.setTime(date);

        return cal;
    }

    /**
     * Get the DAY_OF_MONTH
     *
     * @param currDate the date for which the day is required
     * @return the day of the month, the first day is 1
     */
    public static int getDay(Date currDate) {
        if (currDate == null) {
            return -1;
        }

        return new DateTime(currDate).getDayOfMonth();
    }

    /**
     * Get the DAY_OF_WEEK
     *
     * @param currDate the date for which the day is required
     * @return the day of the week, Mon is 1, Tues is 2 etc.
     */
    public static int getDayOfWeek(Date currDate) {
        if (currDate == null) {
            return -1;
        }

        return new DateTime(currDate).getDayOfWeek();
    }

    /**
     * <p>Extract a specific date part (month, day, or year) from the
     * string representation provided. The part values specifically
     * are represented by <code>Calendar</code> constants: MONTH,
     * YEAR, DAY_OF_MONTH.</p>
     *
     * @param date  the date from which we are reading a field
     * @param field Calendar date/time field
     * @return Date part
     */
    public static int getDatePart(Date date, int field) {
        Calendar cal = calendar(date);

        return cal.get(field);
    }

    /**
     * Returns the date only portion hashcode for the specified date.
     * NOTE: The hashcode returned isn't the hashcode of the date but
     * rather just the date-portion of the date.
     *
     * @param date The Date to get the date-only portion of the hashcode.
     * @return the date-only portion of the hashcode.
     */
    public static int getDateHashCode(Date date) {
        return (date == null) ? 0
                : createDateFormat(timeZoneFactory.getTimeZone()).format(date)
                .hashCode();
    }

    /**
     * Get the difference between two dates in years, taking into account
     * <p/>
     * only their year fields.
     *
     * @param date1 The date to subtract from
     * @param date2 The date to be subtracted
     * @param tz    The time zone in which the calculation is to be made.
     * @return The difference in months of date1 - date2.
     */
    public static int getDateDiffYears(Date date1, Date date2, TimeZone tz) {
        DateTimeZone dTimeZone = null;

        if ((tz != null) && !tz.getID().equals(defaultTimeZoneId)) {
            dTimeZone = DateTimeZone.forTimeZone(tz);
        }

        DateTime dt1 = new DateTime(truncate(date1, Calendar.YEAR), dTimeZone);
        DateTime dt2 = new DateTime(truncate(date2, Calendar.YEAR), dTimeZone);
        Years y = Years.yearsBetween(dt2, dt1);

        return y.getYears();
    }

    /**
     * Get the difference between two dates in days, taking into account
     * <p/>
     * only their day, month, and year fields.
     *
     * @param date1 The date to subtract from
     * @param date2 The date to be subtracted
     * @param tz    The time zone in which the calculation is to be made.
     * @return The difference in months of date1 - date2.
     */
    public static int getDateDiffDays(Date date1, Date date2, TimeZone tz) {
        DateTimeZone dTimeZone = null;

        if ((tz != null) && !tz.getID().equals(defaultTimeZoneId)) {
            dTimeZone = DateTimeZone.forTimeZone(tz);
        }

        DateTime dt1 = new DateTime(truncate(date1, Calendar.DATE), dTimeZone);
        DateTime dt2 = new DateTime(truncate(date2, Calendar.DATE), dTimeZone);
        Days d = Days.daysBetween(dt2, dt1);

        return d.getDays();
    }

    /**
     * Get the difference between two dates in days, taking into account
     * <p/>
     * only their day, month, and year fields. Uses the timezone currently cached in the
     * thread local timezone variable
     *
     * @param date1 The date to subtract from
     * @param date2 The date to be subtracted
     * @return The difference in months of date1 - date2.
     */
    public static int getDateDiffDays(Date date1, Date date2) {
        return getDateDiffDays(date1, date2, timeZoneFactory.getTimeZone());
    }

    public static int getDateDiffDays(Calendar cal1, Calendar cal2) {
        return getDateDiffDays(
                cal1.getTime(),
                cal2.getTime(),
                cal1.getTimeZone());
    }

    public static Calendar addMonthsToCal(Calendar date,
                                          int months,
                                          int dayOfMonth) {
        DateTimeZone dTimeZone = null;
        TimeZone tz = date.getTimeZone();

        if ((tz != null) && !tz.getID().equals(defaultTimeZoneId)) {
            dTimeZone = DateTimeZone.forTimeZone(tz);
        }

        MutableDateTime dateTime = new MutableDateTime(date, dTimeZone);
        dateTime.monthOfYear().add(months);

        int maxDaysOfMonth = dateTime.dayOfMonth().getMaximumValue();
        dateTime.setDayOfMonth(
                (dayOfMonth > maxDaysOfMonth) ? maxDaysOfMonth : dayOfMonth);

        return dateTime.toGregorianCalendar();
    }

    /**
     * @param date
     * @param months
     * @param tz
     * @return The resulting date after addition.
     */
    public static Date addMonths(Date date,
                                 int months,
                                 int dayOfMonth,
                                 TimeZone tz) {
        DateTimeZone dTimeZone = null;

        if ((tz != null) && !tz.getID().equals(defaultTimeZoneId)) {
            dTimeZone = DateTimeZone.forTimeZone(tz);
        }

        MutableDateTime dateTime = new MutableDateTime(date, dTimeZone);
        dateTime.monthOfYear().add(months);

        int maxDaysOfMonth = dateTime.dayOfMonth().getMaximumValue();
        dateTime.setDayOfMonth(
                (dayOfMonth > maxDaysOfMonth) ? maxDaysOfMonth : dayOfMonth);

        return dateTime.toDate();
    }

    /**
     * @param amortizationStartDate
     * @param tz
     * @return The day of month.
     */
    public static int getDayOfMonth(Date amortizationStartDate, TimeZone tz) {
        DateTimeZone dTimeZone = null;

        if ((tz != null) && !tz.getID().equals(defaultTimeZoneId)) {
            dTimeZone = DateTimeZone.forTimeZone(tz);
        }

        return new DateTime(amortizationStartDate, dTimeZone).getDayOfMonth();
    }

    /**
     * @param date
     * @param tz
     * @return The day of year.
     */
    public static int getMonthOfYear(Date date, TimeZone tz) {
        DateTimeZone dTimeZone = null;

        if ((tz != null) && !tz.getID().equals(defaultTimeZoneId)) {
            dTimeZone = DateTimeZone.forTimeZone(tz);
        }

        return new DateTime(date, dTimeZone).getMonthOfYear();
    }

    /**
     * @param date
     * @param tz
     * @return The year.
     */
    public static int getYear(Date date, TimeZone tz) {
        DateTimeZone dTimeZone = null;

        if ((tz != null) && !tz.getID().equals(defaultTimeZoneId)) {
            dTimeZone = DateTimeZone.forTimeZone(tz);
        }

        return new DateTime(date, dTimeZone).getYear();
    }

    /**
     * Set the day of the month on the date
     *
     * @param date
     * @param day
     * @return The resulting date.
     */
    public static Date setDayOfMonth(Date date, int day) {
        int maxDayOfMonth = DateUtil.getMaximumDayOfMonth(date);

        if (day >= maxDayOfMonth) {
            return setToEndOfMonth(date);
        } else {
            DateTime dt = new DateTime(date);

            return dt.dayOfMonth().setCopy(day).toDate();
        }
    }

    /**
     * Set the month of the year on the date
     *
     * @param date
     * @param month
     * @return The resulting date.
     */
    public static Date setMonthOfYear(Date date, int month) {
        DateTime dt = new DateTime(date);

        return dt.monthOfYear().setCopy(month).toDate();
    }

    /**
     * @param date
     * @param years
     * @param tz
     * @return The resulting date after addition.
     */
    public static Date addYears(Date date, int years, TimeZone tz) {
        DateTimeZone dTimeZone = null;

        if ((tz != null) && !tz.getID().equals(defaultTimeZoneId)) {
            dTimeZone = DateTimeZone.forTimeZone(tz);
        }

        MutableDateTime dateTime = new MutableDateTime(date, dTimeZone);
        dateTime.year().add(years);

        return dateTime.toDate();
    }

    /**
     * Determine which date is earlier
     *
     * @param date1
     * @param date2
     * @return The min between the 2 dates.
     */
    public static Date min(Date date1, Date date2) {
        return date1.before(date2) ? date1 : date2;
    }

    /**
     * Determine which date is later
     *
     * @param date1
     * @param date2
     * @return The max between the 2 dates.
     */
    public static Date max(Date date1, Date date2) {
        return date1.after(date2) ? date1 : date2;
    }

    /**
     * Adds the given number of whole days to the supplied date
     *
     * @param date
     * @param days
     * @return The resulting date after the addition of days.
     */
    public static Date addDays(Date date, int days) {
        DateTime dt = new DateTime(truncate(date, Calendar.DATE));

        return dt.plusDays(days).toDate();
    }

    /**
     * Subtract the given number of whole days from the supplied date
     *
     * @param date
     * @param days
     * @return The resulting date after the subtraction of days.
     */
    public static Date minusDays(Date date, int days) {
        DateTime dt = new DateTime(truncate(date, Calendar.DATE));

        return dt.minusDays(days).toDate();
    }

    /**
     * Sets it to the end of the month
     *
     * @param date
     * @return The resulting date.
     */
    public static Date setToEndOfMonth(Date date) {
        DateTime dt = new DateTime(truncate(date, Calendar.DATE));
        int maxDate = dt.dayOfMonth().getMaximumValue();

        return dt.dayOfMonth().setCopy(maxDate).toDate();
    }

    /**
     * Gets the maximum day of month of year.
     * @param date
     * @return maximum day of month.
     */
    public static int getMaximumDayOfMonth(Date date) {
        DateTime dt = new DateTime(truncate(date, Calendar.DATE));

        return dt.dayOfMonth().getMaximumValue();
    }

    public static Date setToEndOfQuarter(Date date) {
        Calendar cal = calendar(date);
        cal.set(Calendar.DAY_OF_MONTH, 1);

        while ((cal.get(Calendar.MONTH) % 3) != 2) {
            cal.add(Calendar.MONTH, 1);
        }

        cal.set(
                Calendar.DAY_OF_MONTH,
                cal.getActualMaximum(Calendar.DAY_OF_MONTH));

        return cal.getTime();
    }

    public static Date setToEndOfYear(Date date) {
        Calendar cal = calendar(date);
        cal.set(Calendar.MONTH, 11);
        cal.set(
                Calendar.DAY_OF_MONTH,
                cal.getActualMaximum(Calendar.DAY_OF_MONTH));

        return cal.getTime();
    }

    /**
     * <p>Checks if two date objects are on the same day ignoring time.</p>
     *
     * <p>28 Mar 2002 13:45 and 28 Mar 2002 06:01 would return true.
     * 28 Mar 2002 13:45 and 12 Mar 2002 13:45 would return false.
     * </p>
     *
     * @param date1 the first date, not altered, not null
     * @param date2 the second date, not altered, not null
     * @return true if they represent the same day
     * @throws IllegalArgumentException if either date is <code>null</code>
     * @since 2.1
     */
    public static boolean isSameDay(Date date1, Date date2) {
        if ((date1 == null) || (date2 == null)) {
            throw new IllegalArgumentException("The date must not be null");
        }

        return isSameDay(calendar(date1), calendar(date2));
    }

    /**
     * <p>Checks if two calendar objects are on the same day ignoring time.</p>
     *
     * <p>28 Mar 2002 13:45 and 28 Mar 2002 06:01 would return true.
     * 28 Mar 2002 13:45 and 12 Mar 2002 13:45 would return false.
     * </p>
     *
     * @param cal1 the first calendar, not altered, not null
     * @param cal2 the second calendar, not altered, not null
     * @return true if they represent the same day
     * @throws IllegalArgumentException if either calendar is <code>null</code>
     * @since 2.1
     */
    public static boolean isSameDay(Calendar cal1, Calendar cal2) {
        if ((cal1 == null) || (cal2 == null)) {
            throw new IllegalArgumentException("The date must not be null");
        }

        return ((cal1.get(Calendar.ERA) == cal2.get(Calendar.ERA)) &&
                (cal1.get(Calendar.YEAR) == cal2.get(Calendar.YEAR)) &&
                (cal1.get(Calendar.DAY_OF_YEAR) == cal2
                        .get(Calendar.DAY_OF_YEAR)));
    }

    /**
     * True if the month and year are the same for the two dates
     *
     * @param dt1
     * @param dt2
     * @return True if the 2 dates have the same month.
     */
    public static boolean isSameMonth(Date dt1, Date dt2) {
        DateTime t1 = new DateTime(dt1);
        DateTime t2 = new DateTime(dt2);

        return ((t1.getMonthOfYear() == t2.getMonthOfYear()) &&
                (t1.getYear() == t2.getYear()));
    }

    /**
     * Utility method that handles date comparisons and takes into
     * consideration null values.
     *
     * @param date1
     * @param date2
     * @return True if the 2 dates are equal.
     * @todo Hibernate apparently creates loans that it has not set the attributes on and
     * adds them to a list somewhere. If these checks are not in place the system will fail.
     */
    public static boolean isDateEquals(Date date1, Date date2) {
        if ((date1 == null) && (date2 == null)) {
            return true;
        }

        if ((date1 == null) || (date2 == null)) {
            return false;
        }

        DateTime dt1 = new DateTime(date1);
        DateTime dt2 = new DateTime(date2);

        return dt1.isEqual(dt2);
    }

    /**
     * Returns true if the <code>date</code> is >=effectiveDate and <endDate
     * @param date
     * @param effectiveDate
     * @param endDate
     * @return True if the date is between the effective and end dates.
     */
    public static boolean isBetween(Date date,
                                    Date effectiveDate,
                                    Date endDate) {
        Interval v = new Interval(effectiveDate.getTime(), endDate.getTime());

        return v.contains(date.getTime());
    }

    public static Date truncate(Date dt, int type) {
        DateTime d = new DateTime(dt);

        if (type == Calendar.DATE) {
            //In many places DST is used, where the local clock moves forward
            //by an hour in spring and back by an hour in autumn/fall.
            //This means that in spring, there is a "gap" where a local time does not exist.
            //Just to handle , if exception comes up we will return Date with UTC
            try {
                return new DateTime(
                        d.getYear(),
                        d.getMonthOfYear(),
                        d.getDayOfMonth(),
                        0,
                        0,
                        0,
                        0).toDate();
            } catch (IllegalArgumentException ex) {
                // Date with UTC Timezone
                final DateTimeZone dtz = DateTimeZone.forID("UTC");

                return new org.joda.time.LocalDateTime(
                        d.getYear(),
                        d.getMonthOfYear(),
                        d.getDayOfMonth(),
                        0,
                        0,
                        0,
                        0).toDateTime(dtz).toDate();
            }
        } else if (type == Calendar.YEAR) {
            return new DateTime(d.getYear(), 1, 1, 0, 0, 0, 0).toDate();
        } else {
            return org.apache.commons.lang.time.DateUtils.truncate(dt, type);
        }
    }

    public static String getTimeDiff(long time) {
        String format = String.format("%%0%dd", 2);
        time = time / 1000;
        String seconds = String.format(format, time % 60);
        String minutes = String.format(format, (time % 3600) / 60);
        String hours = String.format(format, time / 3600);

        return hours + ":" + minutes + ":" + seconds;
    }


    /**
     * Formats the specified date in the form MM/dd/yyyy HH:mm:ss.
     * @param date The date to format.
     * @return The formatted date in the form MM/dd/yyyy HH:mm:ss.
     */
    public static String formatTimeStamp(Date date) {
        return defaultTimeStampFormat.print(new DateTime(date));
    }

    /**
     * Return Date with End Of Month day like for March it will set
     * day to 31 and April it would set 30
     * @param date
     * @return
     */
    public static Date getEndofMonth(Date date) {
        if (date == null) {
            return null;
        }

        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        cal.set(
                Calendar.DAY_OF_MONTH,
                cal.getActualMaximum(Calendar.DAY_OF_MONTH));

        return cal.getTime();
    }

    /**
     * Return Quater Date of given Date like If Feb is being passed
     * it will return 31st March if November is given 31 December
     * @param date
     * @return
     */
    public static Date getEndofQuarter(Date date) {
        if (date == null) {
            return null;
        }

        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        int currMonth = cal.get(Calendar.MONTH);
        int endOfQuarterMonth = Calendar.MARCH;

        if (currMonth > Calendar.MARCH) {
            if (currMonth <= Calendar.JUNE) {
                endOfQuarterMonth = Calendar.JUNE;
            } else if (currMonth <= Calendar.SEPTEMBER) {
                endOfQuarterMonth = Calendar.SEPTEMBER;
            } else {
                endOfQuarterMonth = Calendar.DECEMBER;
            }
        }

        //first set the day of the month to be the first for the following reason:
        //if the date is the 31st and quarter end is on the 30th,
        //setting only the month actually skips you one month ahead
        //setting the day to the first prevents that
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.MONTH, endOfQuarterMonth);

        cal.set(
                Calendar.DAY_OF_MONTH,
                cal.getActualMaximum(Calendar.DAY_OF_MONTH));

        return cal.getTime();
    }

    /**
     * Return Starting Quater Date of given Date like If Feb is being passed
     * it will return 1st January if November is given 1st October
     * @param date
     * @return
     */
    public static Date getStartofQuarter(Date date) {
        if (date == null) {
            return null;
        }

        Calendar cal = Calendar.getInstance();
        cal.setTime(date);

        int currMonth = cal.get(Calendar.MONTH);
        int startOfQuarterMonth = Calendar.JANUARY;

        if (currMonth > Calendar.MARCH) {
            if (currMonth <= Calendar.JUNE) {
                startOfQuarterMonth = Calendar.APRIL;
            } else if (currMonth <= Calendar.SEPTEMBER) {
                startOfQuarterMonth = Calendar.JULY;
            } else {
                startOfQuarterMonth = Calendar.OCTOBER;
            }
        }

        //first set the day of the month
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.MONTH, startOfQuarterMonth);

        return cal.getTime();
    }

    /**
     * Convert integer to Date fomrat, like Integer valud would be
     * 20121231 it will be converted as Date format.
     * @param date
     * @return
     */
    public static Date convertToDateFromYYYYMMDD(int date) {
        DateFormat pattern = new SimpleDateFormat("yyyyMMdd");

        return parse(pattern, String.valueOf(date));
    }

    /**
     * Parse String to Date
     * @param sdf
     * @param val
     * @return
     */
    public static Date parse(DateFormat sdf, String val) {
        if (val == null) {
            return null;
        }

        ParsePosition p = new ParsePosition(0);
        Date d = sdf.parse(val, p);

        return (p.getIndex() == 0) ? null : d;
    }

    /**
     * Parse String into Date format
     * @param val
     * @return
     */
    public static Date convertToJAVADateFromYYYYMMDD(String val) {
        SimpleDateFormat pattern = new SimpleDateFormat("yyyyMMdd");

        return parse(pattern, val);
    }

    /**
     * Convert Date format into Integer
     * @param dt
     * @return
     */
    public static int convertToIntYYYYMMDDFromJavaDate(Date dt) {
        SimpleDateFormat pattern = new SimpleDateFormat("yyyyMMdd");
        StringBuffer buff = new StringBuffer();

        buff = pattern.format(dt, buff, new FieldPosition(0));

        return Integer.valueOf(buff.toString());
    }

    /**
     * onvert Date format into Integer
     * @param dt
     * @param dateFormat
     * @return
     */
    public static int convertToIntFromJavaDate(Date dt, String dateFormat) {
        SimpleDateFormat pattern = new SimpleDateFormat(dateFormat);
        StringBuffer buff = new StringBuffer();

        buff = pattern.format(dt, buff, new FieldPosition(0));

        return Integer.valueOf(buff.toString());
    }

    /**
     * Convert Date format to String
     * @param dt
     * @return
     */
    public static String convertToYYYYMMDDFromJavaDate(Date dt) {
        SimpleDateFormat pattern = new SimpleDateFormat("yyyyMMdd");
        StringBuffer buff = new StringBuffer();

        buff = pattern.format(dt, buff, new FieldPosition(0));

        return buff.toString();
    }

    /**
     * Increment date by given Months, it will always increment Date
     * even if given with negative number. Since we don't use Days in calculation
     * we are not adjusting Day of Date.
     * @param dt
     * @param increment
     * @return
     */
    public static Date incrementMonth(Date dt, int increment) {
        if (dt == null) {
            return null;
        }

        if (increment < 0) {
            increment = -1 * increment;
        }

        Calendar calendar = calendar(dt);
        calendar.add(Calendar.MONTH, increment);

        return calendar.getTime();
    }

    /**
     * Decrement date by given Months, it will always decrement Date
     * even if given with positive number. Since we don't use Days in calculation
     * we are not adjusting Day of Date.
     * @param dt
     * @param increment
     * @return
     */
    public static Date decrementMonth(Date dt, int decrement) {
        if (dt == null) {
            return null;
        }

        if (decrement > 0) {
            decrement = -1 * decrement;
        }

        Calendar calendar = calendar(dt);
        calendar.add(Calendar.MONTH, decrement);

        return calendar.getTime();
    }

    /**
     * This methods calculate the Next date from given date after moving the
     * given months.
     *
     * @param date - from which to move
     * @param monthsToMove - months to move
     * @param isHistoric - move backward in history or forward in future
     * @param getMonthEndDate - get the new date with Last day of Month else will keep the day specified in the given date
     * @return
     */
    public static int getNextDate(int date,
                                  int monthsToMove,
                                  boolean isHistoric,
                                  boolean getMonthEndDate) {
        // convert date1 to months:
        int months = (date / 10000 * 12) + ((date / 100) % 100);

        // get months of date2
        int resultMonths;

        // if history then subtract otherwise add index ( months)
        if (isHistoric) {
            resultMonths = months - monthsToMove;
        } else {
            resultMonths = months + monthsToMove;
        }

        // get the date2 in yyyymm
        int mm = resultMonths % 12;
        int yyyy = resultMonths / 12;

        if (mm == 0) {
            yyyy -= 1;
            mm = 12;
        }

        int yyyymm = (yyyy * 100) + mm;

        int dd = 0;

        if (getMonthEndDate) {
            // now figure out what to do with dd part, be careful, Feb has only 28 or 29 days, so if the date1=20000131, Index =1, then you have to handle it properly. the dumbest way is this, but fix it before using:
            dd = getDaysOfMonth(mm, yyyy);
        } else {
            // Keep the day in the given date
            dd = date % 100;

            int expectedEndDate = getDaysOfMonth(mm, yyyy);
            dd = Math.min(dd, expectedEndDate); // Limit to expected month end
        }

        return (yyyymm * 100) + dd;
    }

    /**
     * return day w.r.t Month/year
     * @param month
     * @param year
     * @return
     */
    public static int getDaysOfMonth(int month, int year) {
        int day = 31;

        switch (month) {
            case 2:
                day = ((year % 4) == 0) ? 29 : 28;

                break;

            case 4:
            case 6:
            case 9:
            case 11:
                day = 30;

                break;

            default:
                day = 31;
        }

        return day;
    }

    /**
     * Function to find out month end date or not
     * @param date
     * @return true in case provided date is month end date other wise false
     */
    public static boolean isMonthEndDate(Date date) {
        int month = getMonthForDate(date) + 1; // adding 1 since month start with 0
        int year = getYearForDate(date);
        int day = getDay(date);

        return day == getDaysOfMonth(month, year);
    }

    /**
     * return the month number for the date.
     * Jan = 0, Feb = 1, etc.
     *
     * @param dateYYYYMMDD
     * @return
     */
    public static int getMonthForDate(int dateYYYYMMDD) {
        return ((dateYYYYMMDD / 100) % 100) - 1;
    }

    /**
     * return the date.
     * @param date
     * @return
     */
    public static String formatTimeStampExtended(Date date) {
        SimpleDateFormat format =
                new SimpleDateFormat(DATE_FORMAT_EXTENDED_WITH_TIMEZONE);

        return format.format(date);
    }

    /**
     * return the date from string.
     * @param date
     * @return
     */
    public static Date formatStringToDate(String date, String pattern) {
        SimpleDateFormat formatter = new SimpleDateFormat(pattern);

        try {
            return formatter.parse(date);
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * return the string from Date.
     * @param date
     * @return
     */
    public static String formatDateToString(Date date, String pattern) {
        SimpleDateFormat formatter = new SimpleDateFormat(pattern);

        return formatter.format(date);
    }

    /**
     * Converts the date string from source pattern to the target pattern.
     * Keeps the date string as a string.
     * @param date Date string that needs to be converted.
     * @param sourcePattern Source pattern.
     * @param targetPatter Target pattern.
     * @return Date as a string converted in to target pattern.
     */
    public static String formatStringDateToStringDate(String date,
                                                      String sourcePattern,
                                                      String targetPatter) {
        Date sourceDate = formatStringToDate(date, sourcePattern);

        return formatDateToString(sourceDate, targetPatter);
    }

    /**
     * A utility function the check if current time has not exceeded the timeout given the start time.
     *
     * @param timeout max time to wait
     * @param unit Time unit.
     * @param startTimestamp process start time.
     * @return true if process is not times out. False other wise.
     */
    public static boolean isWithinTime(long timeout,
                                       TimeUnit unit,
                                       Date startTimestamp) {
        Date currentTime = new Date();
        long timeoutInMilliseconds = unit.toMillis(timeout);
        long timeSpentInMilliseconds =
                currentTime.getTime() - startTimestamp.getTime();

        if (timeSpentInMilliseconds > timeoutInMilliseconds) {
            return false;
        }

        return true;
    }

    public static int years_between(Date date2, Date date1) {
        Calendar cal1 = Calendar.getInstance();
        cal1.setTime(date1);
        Calendar cal2 = Calendar.getInstance();
        cal2.setTime(date2);

        return cal2.get(Calendar.YEAR) - cal1.get(Calendar.YEAR) -
                ((cal2.get(Calendar.MONTH) < cal1.get(Calendar.MONTH)) ? 1 : 0);
    }

    /**
     * <p>Parses a string representing a date by trying a variety of different parsers, but returns
     * with a one which has a year greater than 1970
     * </p>
     *
     * <p>The parse will try each parse pattern in turn.
     * A parse is only deemed successful if it parses the whole of the input string.
     * and year is greater than 1970
     * If no parse patterns match greater than 1970, a ParseException is thrown.</p>
     *
     * @param str  the date to parse, not null
     * @param parsePatterns  the date format patterns to use, see SimpleDateFormat, not null
     * @return the parsed date
     * @throws IllegalArgumentException if the date string or pattern array is null
     * @throws ParseException if none of the date patterns were suitable
     * @see Calendar#isLenient()
     */
    public static Date parseDateWithConsideringYear(final String str,
                                                    final String[] parsePatterns) throws ParseException {
        Date dateParse = null;

        if ((str == null) || (parsePatterns == null)) {
            throw new IllegalArgumentException(
                    "Date and Patterns must not be null");
        }

        for (final String parsePattern : parsePatterns) {
            try {
                logger.info("trying to parse date with format," + parsePattern);

                dateParse =
                        DateUtil.parseDate(str, new String[] { parsePattern });

                if (DateUtil.getYear(dateParse, null) >= 1970) {
                    return dateParse;
                } else {
                    continue;
                }
            } catch (ParseException px) {
                logger.info(
                        "parse exception while parsing with pattern," +
                                parsePattern);

                continue;
            }
        }

        logger.info("Could not parse date with a year greater than 1970");
        throw new ParseException("Unable to parse the date: " + str, -1);
    }

    public static int getMonthForDate(Date date) {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        int month = calendar.get(Calendar.MONTH);

        return month;
    }

    public static int getYearForDate(Date date) {
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        int year = calendar.get(Calendar.YEAR);

        return year;
    }

    public static String getMonthForInt(int num) {
        String month = "wrong";
        DateFormatSymbols dfs = new DateFormatSymbols();
        String[] months = dfs.getMonths();

        if ((num >= 0) && (num <= 11)) {
            month = months[num];
        }

        return month;
    }

    public static String getQuarterFromMonth(int month) {
        return ((month >= Calendar.JANUARY) && (month <= Calendar.MARCH))
                ? "1st"
                : (((month >= Calendar.APRIL) && (month <= Calendar.JUNE))
                ? "2nd"
                : (((month >= Calendar.JULY) &&
                (month <= Calendar.SEPTEMBER)) ? "3rd"
                : "4th"));
    }

    /**
     * Generates a string which first converts quarter string to [Q#] format and then
     * appends it with year in the format [YYYYQQ]
     * @param quarter
     * @param year
     * @return
     */
    public static String generateQuarterYearString(String quarter,
                                                   String year) {
        return year + (quarter.equals("1st") ? "Q1"
                : (quarter.equals("2nd") ? "Q2"
                : (quarter.equals("3rd") ? "Q3"
                : (quarter.equals("4th") ? "Q4" : ""))));
    }

    public static Date getNextDate(Date startDate, int months) {
        return DateUtil.convertToDateFromYYYYMMDD(
                DateUtil.getNextDate(
                        DateUtil.convertToIntYYYYMMDDFromJavaDate(startDate),
                        months,
                        false,
                        true));
    }

    /**
     * This method will convert the provided date into the
     * start date of the current month
     *
     * @param date
     * @return returns the updated date which has start day of the month
     */
    public static Date convertToStartDate(Date date) {
        Calendar calendarDate = calendar(date);
        calendarDate.set(Calendar.DAY_OF_MONTH, 1);
        return calendarDate.getTime();
    }

    public static java.sql.Date getJavaSQLDate(java.util.Date utilDate) {
        return new java.sql.Date(utilDate.getTime());
    }

    public static LocalDateTime getDateTime() {
        // Current epoch time in milliseconds
        long epochTimeMillis = System.currentTimeMillis();

        // Convert epoch time to LocalDateTime
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochTimeMillis), ZoneId.systemDefault());
        return dateTime;
    }

    public static Date parseDate(String strDate) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return parseDate(strDate,dateFormat);
    }

    public static Date parseDate(String strDate, SimpleDateFormat dateFormat) throws ParseException {
        return dateFormat.parse(strDate);
    }

    public static Date convertToDate(LocalDate localDate) {
        // Convert LocalDate to Date using atStartOfDay() and then toInstant()
        return Date.from(localDate.atStartOfDay(ZoneId.systemDefault()).toInstant());
    }

    public static LocalDate convertToLocalDate(Date date) {
        // Convert Date to Instant
        Instant instant = date.toInstant();

        // Create LocalDate from Instant
        return instant.atZone(ZoneId.systemDefault()).toLocalDate();
    }

    public static Date convertDateToIST(Date date) throws ParseException {
        int month = DateUtil.getMonthForDate(date) + 1;
        int year = DateUtil.getYearForDate(date);
        int day = DateUtil.getDay(date);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("IST"));
        return sdf.parse(year + "-" + month + "-" + day + "T20:20:25.927Z");
    }

    public static String getAccountingPeriod(Date date) {
        int year = getYearForDate(date);
        int month = getMonthForDate(date);
        return year + "-" + (month+1);
    }
}

