import { max, scaleBand, scaleLinear } from 'd3';
import { useEffect, useId, useMemo, useRef, useState } from 'react';
import {
  formatJakartaBucketRange,
  formatJakartaBucketStart,
  type MetricBucket,
} from '../lib/metricBuckets';

interface MetricBarChartProps {
  buckets: readonly MetricBucket[];
  title: string;
  formatValue: (value: number) => string;
}

const CHART_HEIGHT = 150;
const MARGIN = { top: 12, right: 8, bottom: 32, left: 64 };

export function MetricBarChart({ buckets, title, formatValue }: MetricBarChartProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  const [activeWindowEnd, setActiveWindowEnd] = useState<string | null>(null);
  const titleId = useId();
  const tooltipId = useId();

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return undefined;

    const updateWidth = () => setWidth(container.clientWidth);
    updateWidth();
    const observer = new ResizeObserver(updateWidth);
    observer.observe(container);
    return () => observer.disconnect();
  }, []);

  const chart = useMemo(() => {
    const innerWidth = Math.max(1, width - MARGIN.left - MARGIN.right);
    const innerHeight = CHART_HEIGHT - MARGIN.top - MARGIN.bottom;
    const maxValue = Math.max(1, max(buckets, (bucket) => bucket.value) ?? 0);
    const x = scaleBand<string>()
      .domain(buckets.map((bucket) => bucket.windowEnd))
      .range([0, innerWidth])
      .padding(0.18);
    const y = scaleLinear().domain([0, maxValue]).nice().range([innerHeight, 0]);
    return { innerHeight, x, y, yTicks: y.ticks(3) };
  }, [buckets, width]);

  const activeBucket = activeWindowEnd === null
    ? null
    : buckets.find((bucket) => bucket.windowEnd === activeWindowEnd) ?? null;
  const tooltipText = activeBucket
    ? `${formatJakartaBucketRange(activeBucket.windowEnd) ?? activeBucket.windowEnd}: ${formatValue(activeBucket.value)}`
    : 'Focus or hover a bar to inspect its five-minute aligned window.';
  const xLabelStep = Math.max(1, Math.ceil(buckets.length / 6));

  return <div className="metric-bucket-chart" ref={containerRef}>
    <svg
      className="metric-bucket-chart-svg"
      viewBox={`0 0 ${Math.max(width, 1)} ${CHART_HEIGHT}`}
      role="group"
      aria-labelledby={titleId}
    >
      <title id={titleId}>{title}</title>
      <g transform={`translate(${MARGIN.left},${MARGIN.top})`}>
        <line className="metric-bucket-axis" x1={0} x2={chart.x.range()[1]} y1={chart.innerHeight} y2={chart.innerHeight} />
        {chart.yTicks.map((tick) => <g className="metric-bucket-y-tick" key={tick} transform={`translate(0,${chart.y(tick)})`}>
          <line x1={0} x2={chart.x.range()[1]} />
          <text x={-6} dy="0.32em" textAnchor="end">{formatValue(tick)}</text>
        </g>)}
        {buckets.map((bucket, index) => {
          const x = chart.x(bucket.windowEnd) ?? 0;
          const barHeight = chart.innerHeight - chart.y(bucket.value);
          const range = formatJakartaBucketRange(bucket.windowEnd) ?? bucket.windowEnd;
          const label = `${range}: ${formatValue(bucket.value)}`;
          return <g key={bucket.windowEnd}>
            <rect
              className="metric-bucket-hit-target"
              x={x}
              y={0}
              width={chart.x.bandwidth()}
              height={chart.innerHeight}
              tabIndex={0}
              aria-label={label}
              aria-describedby={tooltipId}
              onBlur={() => setActiveWindowEnd(null)}
              onFocus={() => setActiveWindowEnd(bucket.windowEnd)}
              onMouseEnter={() => setActiveWindowEnd(bucket.windowEnd)}
              onMouseLeave={() => setActiveWindowEnd(null)}
            >
              <title>{label}</title>
            </rect>
            <rect
              className="metric-bucket-bar"
              x={x}
              y={chart.y(bucket.value)}
              width={chart.x.bandwidth()}
              height={barHeight}
              pointerEvents="none"
              aria-hidden="true"
            >
              <title>{label}</title>
            </rect>
            {(index % xLabelStep === 0 || index === buckets.length - 1) && <text
              className="metric-bucket-x-label"
              x={x + chart.x.bandwidth() / 2}
              y={chart.innerHeight + 16}
              textAnchor="middle"
            >{formatJakartaBucketStart(bucket.windowEnd) ?? '—'}</text>}
          </g>;
        })}
      </g>
    </svg>
    <output className="metric-bucket-tooltip" id={tooltipId} aria-live="polite">{tooltipText}</output>
  </div>;
}
