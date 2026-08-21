export type AutocompleteItem = any;
export type Component = any;
export type EditorTheme = any;

export class Editor {
  text = "";
  focused = false;
  onSubmit?: (value: string) => void;

  setText(value: string) {
    this.text = value;
  }
  handleInput(data: string) {
    if (matchesKey(data, "enter")) this.onSubmit?.(this.text);
    else if (matchesKey(data, "backspace")) this.text = this.text.slice(0, -1);
    else if (data.length === 1 && data.charCodeAt(0) >= 32) this.text += data;
  }
  render() {
    return [this.text];
  }
  invalidate() {}
}

export const Key = {
  escape: "escape",
  enter: "enter",
  left: "left",
  right: "right",
  up: "up",
  down: "down",
  space: "space",
  backspace: "backspace",
  shift: (key: string) => `shift+${key}`,
} as const;

export class Text {
  constructor(public text: string = "") {}
}

export class Image {
  readonly args: any[];
  constructor(...args: any[]) {
    this.args = args;
  }
  render() {
    return [];
  }
  invalidate() {}
}

export class Markdown {
  text: string;
  paddingX: number;
  paddingY: number;
  theme: any;
  defaultTextStyle: any;
  options: Record<string, unknown>;

  constructor(text: string, paddingX = 0, paddingY = 0, theme: any = {}, defaultTextStyle?: any, options?: Record<string, unknown>) {
    this.text = text;
    this.paddingX = paddingX;
    this.paddingY = paddingY;
    this.theme = theme;
    this.defaultTextStyle = defaultTextStyle;
    this.options = { ...(options ?? {}) };
  }
}

export function getCapabilities() {
  return { hyperlinks: false, images: null, trueColor: false };
}

export function hyperlink(text: string) {
  return text;
}

export function wrapTextWithAnsi(value: string, width: number) {
  if (width <= 0 || value.length <= width) return [value];
  const lines: string[] = [];
  for (let i = 0; i < value.length; i += width) lines.push(value.slice(i, i + width));
  return lines;
}

export function matchesKey(data: string, key: string) {
  if (data === key) return true;
  if (key === "enter" || key === "return") return data === "\r" || data === "\n";
  if (key === "escape") return data === "\u001b";
  return false;
}

export function isKeyRelease(data: string) {
  return data.includes(":3u") || data.includes(":3~") || data.includes(":3;");
}

export function truncateToWidth(value: string, width: number) {
  return value.length > width ? value.slice(0, width) : value;
}

export function visibleWidth(value: string) {
  return value.length;
}

export function fuzzyFilter<T>(items: T[], query: string, getText: (item: T) => string): T[] {
  const normalized = query.toLocaleLowerCase();
  return items
    .map((item, index) => {
      const text = getText(item).toLocaleLowerCase();
      const direct = text.indexOf(normalized);
      if (direct >= 0) return { item, index, score: direct };
      let cursor = 0;
      for (const character of normalized) {
        cursor = text.indexOf(character, cursor);
        if (cursor < 0) return undefined;
        cursor += 1;
      }
      return { item, index, score: cursor + text.length };
    })
    .filter((value): value is { item: T; index: number; score: number } => Boolean(value))
    .sort((left, right) => left.score - right.score || left.index - right.index)
    .map(({ item }) => item);
}
