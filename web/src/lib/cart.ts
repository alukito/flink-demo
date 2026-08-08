export interface CartLine {
  quantity: number;
}

export function cartItemCount(items: readonly CartLine[]): number {
  return items.reduce((total, item) => total + item.quantity, 0);
}
