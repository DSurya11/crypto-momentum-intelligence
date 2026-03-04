const USD_TO_INR = 83;

export function usdToInr(value: number): number {
  return value * USD_TO_INR;
}

export function formatInrFromUsd(value: number): string {
  const inr = usdToInr(value);

  if (inr < 1) {
    return `₹${inr.toPrecision(3)}`;
  }

  if (inr < 1000) {
    return `₹${inr.toFixed(2)}`;
  }

  return new Intl.NumberFormat("en-IN", {
    style: "currency",
    currency: "INR",
    maximumFractionDigits: 2,
  }).format(inr);
}