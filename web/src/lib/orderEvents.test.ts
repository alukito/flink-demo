import assert from 'node:assert/strict';
import test from 'node:test';
import {
  isBuyerOrderEvent,
  isSellerOrderEvent,
  isShipperQueueEvent,
  type OrderEvent,
} from './orderEvents.ts';

function event(eventType: string, payload: Record<string, unknown>): OrderEvent {
  return {
    event_id: 'event-1',
    event_type: eventType,
    actor_id: 'actor-a',
    actor_role: 'seller',
    timestamp: '2026-08-09T00:00:00Z',
    payload,
  };
}

test('matches buyer order events by UUID rather than a shared display name', () => {
  const checkout = event('cart.checkout', {
    buyer_id: 'buyer-a',
    buyer_name: 'Alex',
    seller_id: 'seller-a',
    seller_name: 'Alex',
  });

  assert.equal(isBuyerOrderEvent(checkout, 'buyer-a'), true);
  assert.equal(isBuyerOrderEvent(checkout, 'buyer-b'), false);
});

test('does not refresh seller-b for seller-a delivery events', () => {
  const delivered = event('shipment.delivered', {
    buyer_id: 'buyer-a',
    buyer_name: 'Alex',
    seller_id: 'seller-a',
    seller_name: 'Alex',
  });

  assert.equal(isSellerOrderEvent(delivered, 'seller-a'), true);
  assert.equal(isSellerOrderEvent(delivered, 'seller-b'), false);
});

test('recognizes events which can change the shipper job queue', () => {
  assert.equal(isShipperQueueEvent(event('order.confirmed', {})), true);
  assert.equal(isShipperQueueEvent(event('shipment.picked', {})), true);
  assert.equal(isShipperQueueEvent(event('shipment.delivered', {})), false);
});
