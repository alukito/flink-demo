package com.flinkdemo.level2.function;

import com.flinkdemo.level2.model.EventEnvelope;
import com.flinkdemo.level2.model.WindowStat;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public final class TopProductWindowFunction extends ProcessAllWindowFunction<EventEnvelope, WindowStat, TimeWindow> {
    @Override public void process(Context context, Iterable<EventEnvelope> events, Collector<WindowStat> out) {
        process(context.window(), events, out);
    }

    public void process(TimeWindow window, Iterable<EventEnvelope> events, Collector<WindowStat> out) {
        Map<String, Long> counts = new HashMap<>();
        Map<String, String> names = new HashMap<>();
        for (EventEnvelope event : events) {
            String id = event.getPayload().path("product_id").asText();
            String name = event.getPayload().path("product_name").asText();
            if (!id.isBlank()) { counts.merge(id, 1L, Long::sum); names.put(id, name); }
        }
        String winner = counts.keySet().stream().sorted((a, b) -> {
            int countOrder = Long.compare(counts.get(b), counts.get(a));
            return countOrder != 0 ? countOrder : a.compareTo(b);
        }).findFirst().orElse(null);
        if (winner != null) out.collect(new WindowStat("top_product", "window", Instant.ofEpochMilli(window.getEnd()).toString(), counts.get(winner), Map.of("product_id", winner, "name", names.get(winner))));
    }
}
