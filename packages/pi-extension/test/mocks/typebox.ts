function schema(type: string, options: Record<string, unknown> = {}) {
  return { type, ...options };
}

export const Type = {
  Any: (options?: Record<string, unknown>) => schema("any", options),
  Array: (items: unknown, options?: Record<string, unknown>) => ({ ...schema("array", options), items }),
  Boolean: (options?: Record<string, unknown>) => schema("boolean", options),
  Integer: (options?: Record<string, unknown>) => schema("integer", options),
  Literal: (value: unknown, options?: Record<string, unknown>) => ({ ...schema("literal", options), const: value }),
  Null: (options?: Record<string, unknown>) => schema("null", options),
  Number: (options?: Record<string, unknown>) => schema("number", options),
  Object: (properties: Record<string, unknown>, options?: Record<string, unknown>) => ({
    ...schema("object", options),
    properties,
  }),
  Optional: (inner: unknown) => ({ optional: true, inner }),
  Record: (key: unknown, value: unknown, options?: Record<string, unknown>) => ({
    ...schema("record", options),
    key,
    value,
  }),
  String: (options?: Record<string, unknown>) => schema("string", options),
  Union: (items: unknown[], options?: Record<string, unknown>) => ({ ...schema("union", options), anyOf: items }),
  Unsafe: (options?: Record<string, unknown>) => ({ ...schema("unsafe", options) }),
};
