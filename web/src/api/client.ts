const API_BASE = '/api';

export interface SessionResponse {
  id: string;
  token: string;
  name: string;
  role: string;
}

export async function createSession(name: string, role: string): Promise<SessionResponse> {
  const resp = await fetch(`${API_BASE}/session`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, role }),
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(text || `Failed to create session (status ${resp.status})`);
  }
  return resp.json();
}

export async function apiGet(path: string, token: string): Promise<Response> {
  return fetch(`${API_BASE}${path}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
}

export async function apiPost(path: string, token: string, body?: unknown): Promise<Response> {
  return fetch(`${API_BASE}${path}`, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: body ? JSON.stringify(body) : undefined,
  });
}

// Seller API
export async function addProduct(token: string, name: string, price: number, quantity: number): Promise<Response> {
  return apiPost('/seller/products', token, { name, price, quantity });
}

export async function listSellerProducts(token: string): Promise<Response> {
  return apiGet('/seller/products', token);
}

export async function listSellerOrders(token: string): Promise<Response> {
  return apiGet('/seller/orders', token);
}

export async function confirmOrder(token: string, orderId: string): Promise<Response> {
  return apiPost(`/seller/orders/${orderId}/confirm`, token);
}

// Buyer API
export async function listBuyerProducts(token: string): Promise<Response> {
  return apiGet('/buyer/products', token);
}

export async function addToCart(token: string, cartId: string, productId: string, quantity: number): Promise<Response> {
  return apiPost('/buyer/cart/items', token, { cart_id: cartId, product_id: productId, quantity });
}

export async function checkout(token: string, cartId: string, items: { product_id: string; quantity: number }[], shippingAddress: string): Promise<Response> {
  return apiPost('/buyer/cart/checkout', token, { cart_id: cartId, items, shipping_address: shippingAddress });
}

export async function listBuyerOrders(token: string): Promise<Response> {
  return apiGet('/buyer/orders', token);
}

// Shipper API
export async function listShipperJobs(token: string): Promise<Response> {
  return apiGet('/shipper/jobs', token);
}

export async function pickJob(token: string, orderId: string): Promise<Response> {
  return apiPost(`/shipper/jobs/${orderId}/pick`, token);
}

export async function deliverJob(token: string, orderId: string): Promise<Response> {
  return apiPost(`/shipper/jobs/${orderId}/deliver`, token);
}
