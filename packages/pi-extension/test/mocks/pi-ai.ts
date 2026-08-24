export function StringEnum<const T extends readonly string[]>(values: T, options: Record<string, unknown> = {}) {
  return { type: "string", enum: [...values], ...options };
}
