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

export function secondsUntilReady(readyAt: string | undefined, now: Date): number {
  const readyAtMilliseconds = Date.parse(readyAt ?? '');
  if (!Number.isFinite(readyAtMilliseconds)) return 0;

  return Math.max(0, Math.ceil((readyAtMilliseconds - now.getTime()) / 1000));
}

export function copyDeliveries(deliveries: ShipperDeliveries): ShipperDeliveries {
  return {
    active: [...deliveries.active],
    history: [...deliveries.history],
  };
}
