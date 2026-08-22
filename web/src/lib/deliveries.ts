export interface DeliveryItem {
  product_id: string;
  product_name: string;
  quantity: number;
  unit_price: number;
}

export interface Delivery {
  id: string;
  buyer_id: string;
  buyer_name?: string;
  seller_id: string;
  seller_name?: string;
  items: DeliveryItem[];
  shipping_address: string;
  status: string;
  created_at: string;
  picked_at?: string;
  ready_at?: string;
  delivered_at?: string;
}

export interface ShipperDeliveries {
  active: Delivery[];
  history: Delivery[];
}

export type DeliveryReadiness =
  | { kind: 'unavailable'; label: 'Readiness unavailable' }
  | { kind: 'waiting'; label: string; seconds: number }
  | { kind: 'ready'; label: 'Ready to deliver' };

function parseServerTimestamp(value: string | undefined): number | null {
  if (!value) return null;

  const match = value.match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,9})?(Z|[+-]\d{2}:\d{2})$/,
  );
  if (!match) return null;

  const [, yearText, monthText, dayText, hourText, minuteText, secondText, offset] = match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

  if (
    month < 1 || month > 12
    || day < 1 || day > daysInMonth[month - 1]
    || hour > 23
    || minute > 59
    || second > 59
  ) return null;

  if (offset !== 'Z') {
    const offsetHour = Number(offset.slice(1, 3));
    const offsetMinute = Number(offset.slice(4, 6));
    if (offsetHour > 23 || offsetMinute > 59) return null;
  }

  const milliseconds = Date.parse(value);
  return Number.isFinite(milliseconds) ? milliseconds : null;
}

export function deliveryReadiness(readyAt: string | undefined, now: Date): DeliveryReadiness {
  const readyAtMilliseconds = parseServerTimestamp(readyAt);
  if (readyAtMilliseconds === null) {
    return { kind: 'unavailable', label: 'Readiness unavailable' };
  }

  const seconds = Math.max(0, Math.ceil((readyAtMilliseconds - now.getTime()) / 1000));
  if (seconds > 0) return { kind: 'waiting', seconds, label: `Ready in ${seconds}s` };

  return { kind: 'ready', label: 'Ready to deliver' };
}

export function secondsUntilReady(readyAt: string | undefined, now: Date): number {
  const readiness = deliveryReadiness(readyAt, now);
  return readiness.kind === 'waiting' ? readiness.seconds : 0;
}

export function copyDeliveries(deliveries: ShipperDeliveries): ShipperDeliveries {
  return {
    active: [...deliveries.active],
    history: [...deliveries.history],
  };
}
