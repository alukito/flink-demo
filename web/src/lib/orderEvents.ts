export interface OrderEvent {
  event_id: string;
  event_type: string;
  actor_id: string;
  actor_name?: string;
  actor_role: string;
  timestamp: string;
  payload: Record<string, unknown>;
}

const BUYER_ORDER_EVENT_TYPES = ['cart.checkout', 'order.confirmed', 'shipment.picked', 'shipment.delivered'];
const SELLER_ORDER_EVENT_TYPES = ['cart.checkout', 'order.confirmed', 'shipment.picked', 'shipment.delivered'];
const SHIPPER_QUEUE_EVENT_TYPES = ['order.confirmed', 'shipment.picked'];

export function isBuyerOrderEvent(event: OrderEvent, buyerID: string | null): boolean {
  return buyerID !== null
    && BUYER_ORDER_EVENT_TYPES.includes(event.event_type)
    && event.payload.buyer_id === buyerID;
}

export function isSellerOrderEvent(event: OrderEvent, sellerID: string | null): boolean {
  return sellerID !== null
    && SELLER_ORDER_EVENT_TYPES.includes(event.event_type)
    && event.payload.seller_id === sellerID;
}

export function isShipperQueueEvent(event: OrderEvent): boolean {
  return SHIPPER_QUEUE_EVENT_TYPES.includes(event.event_type);
}
