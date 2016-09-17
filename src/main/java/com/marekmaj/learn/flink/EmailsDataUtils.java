package com.marekmaj.learn.flink;


import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;

final class EmailsDataUtils {

    private EmailsDataUtils() {
    }

    public static String clearEmailAddress(String email) {
        return StringUtils.substringBeforeLast(StringUtils.substringAfterLast(email, "<"), ">");
    }

    public static Boolean isSenderNotABot(String sender) {
        return !"git@git.apache.org".equals(sender) && !"jira@apache.org".equals(sender);
    }

    public static final class MonthFromStringTimestamp {

        private static final DateTimeFormatter INPUT_DATE_TIME_FORMATTER =
                new DateTimeFormatterBuilder()
                        .appendPattern("yyyy-MM-dd-HH:mm:ss")
                        .toFormatter();

        private static final DateTimeFormatter OUTPUT_DATE_TIME_FORMATTER =
                new DateTimeFormatterBuilder()
                        .appendPattern("yyyy-MM")
                        .toFormatter();

        public static String from(String timestamp) {
            return LocalDate.parse(timestamp, INPUT_DATE_TIME_FORMATTER).format(OUTPUT_DATE_TIME_FORMATTER);
        }
    }

}
