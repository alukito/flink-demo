package com.flinkdemo.level2;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

public final class DailyTime {
    private static final ZoneId JAKARTA = ZoneId.of("Asia/Jakarta");

    private DailyTime() {}

    public static String dateKey(String timestamp) {
        return Instant.parse(timestamp).atZone(JAKARTA).toLocalDate().toString();
    }

    public static String windowEnd(String dateKey) {
        return LocalDate.parse(dateKey)
            .plusDays(1)
            .atStartOfDay(JAKARTA)
            .toInstant()
            .toString();
    }
}
