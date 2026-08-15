export interface SignalTraceProps {
  pulseKey: string | number;
}

export function SignalTrace({ pulseKey }: SignalTraceProps) {
  return (
    <span className="signal-trace" aria-hidden="true">
      <span className="signal-trace__pulse" key={pulseKey} />
    </span>
  );
}
