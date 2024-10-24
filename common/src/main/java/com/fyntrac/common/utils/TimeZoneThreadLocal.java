package com.fyntrac.common.utils;

import java.util.TimeZone;

import java.util.TimeZone;

/**
 * <P>
 * This class provides a way of associating a {@link TimeZone} with
 * a thread.
 * </P><P>
 * The system property {@link #ERROR_HANDLING_PROPERTY} controls the
 * error handling behavior of the class.  If this property is set to
 * <code>true</code>, then the thread's time zone must be explicitly
 * set before it
 * it accessed or else an {@link IllegalStateException} will be thrown.
 * Otherwise a default local time zone will be provided.
 * </P>
 */
public class TimeZoneThreadLocal extends InheritableThreadLocal<TimeZone> {
    /**
     * The system property that, when not set to <code>true</code>,
     * will provide a default {@link TimeZone} if one is not yet associated
     * with the current thread.
     */
    private static final String ERROR_HANDLING_PROPERTY =
            System.getProperty("timezonethreadlocal.forceNoDefault");

    /**
     * Get the time zone associated with the current thread.  This method
     * is simply a type-safe wrapper around <code>ThreadLocal.get()</code>.
     * @return The time zone associated with the current thread.
     * @throws IllegalStateException if the {@link #ERROR_HANDLING_PROPERTY}
     * is set to <code>true</code> and no time zone is associated
     * with this thread.
     */
    public TimeZone getTimeZone() {
        return super.get();
    }

    /**
     * Set the time zone associated with the current thread.  This method
     * is simply a type-safe wrapper around <code>ThreadLocal.set()</code>.
     * @param timeZone The time zone associated with the current thread.
     */
    public void setTimeZone(TimeZone timeZone) {
        super.set(timeZone);
    }

    /**
     * Provide an initial value of the time zone the first time it is
     * accessed via {@link ThreadLocal#@get}.
     * @return A new default time zone
     * @throws IllegalStateException If the {@link #ERROR_HANDLING_PROPERTY}
     * is set to <code>true</code>.
     */
    @Override
    protected TimeZone initialValue() {
        if ((ERROR_HANDLING_PROPERTY != null) &&
                ERROR_HANDLING_PROPERTY.equalsIgnoreCase(Boolean.TRUE.toString())) {
            throw new IllegalStateException("No time zone set!");
        } else {
            // Provide a default time zone
            return TimeZone.getDefault();
        }
    }
}