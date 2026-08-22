import { basename } from "node:path";
import type { ExtensionContext } from "@earendil-works/pi-coding-agent";
import { Editor, type EditorTheme, Key, matchesKey, truncateToWidth, visibleWidth } from "@earendil-works/pi-tui";
import type { ActorRecord } from "./protocol";

export type TalkModalResult = { targets: string[]; body: string };

type Recipient = {
  address: string;
  label: string;
  description: string;
  repoPeer: boolean;
};

export function sameRepository(current: ActorRecord, candidate: ActorRecord): boolean {
  if (current.repository_uuid && candidate.repository_uuid) return current.repository_uuid === candidate.repository_uuid;
  if (current.git?.common_dir && candidate.git?.common_dir) return current.git.common_dir === candidate.git.common_dir;
  if (current.jj?.workspace_root && candidate.jj?.workspace_root) return current.jj.workspace_root === candidate.jj.workspace_root;
  return current.cwd === candidate.cwd;
}

export function initialTalkTargets(current: ActorRecord, actors: ActorRecord[], expression?: string): string[] {
  if (!expression) return [];
  if (expression === "--repo") {
    return actors
      .filter((candidate) => candidate.address !== current.address && sameRepository(current, candidate))
      .map((candidate) => candidate.address);
  }
  return expression
    .split(",")
    .map((target) => target.trim())
    .filter(Boolean);
}

function recipient(current: ActorRecord, candidate: ActorRecord): Recipient {
  const label = candidate.alias ? `@${candidate.alias}` : `@${candidate.address}`;
  const vcs = candidate.jj
    ? candidate.jj.change_id.slice(0, 8)
    : candidate.git?.branch
      ? candidate.git.branch
      : candidate.git?.head
        ? candidate.git.head.slice(0, 8)
        : "no-vcs";
  return {
    address: candidate.address,
    label,
    description: `${candidate.harness} · ${candidate.state} · ${basename(candidate.cwd)} · ${vcs}`,
    repoPeer: sameRepository(current, candidate),
  };
}

export async function showTalkModal(
  ctx: ExtensionContext,
  current: ActorRecord,
  actors: ActorRecord[],
  initialTargets: string[] = [],
): Promise<TalkModalResult | null> {
  if (ctx.mode !== "tui") throw new Error("Interactive bus talk modal requires Pi TUI mode");
  const recipients = actors.filter((candidate) => candidate.address !== current.address).map((candidate) => recipient(current, candidate));
  const repoTargets = recipients.filter((candidate) => candidate.repoPeer).map((candidate) => candidate.address);
  if (recipients.length === 0 && initialTargets.length === 0) throw new Error("No other active agents are available on the bus");

  return ctx.ui.custom<TalkModalResult | null>(
    (tui, theme, _keybindings, done) => {
      type Row = { kind: "repo"; label: string; description: string } | { kind: "actor"; recipient: Recipient };
      const rows: Row[] = [
        ...(repoTargets.length > 0
          ? [
              {
                kind: "repo" as const,
                label: `All in this repo (${repoTargets.length})`,
                description: "Fan out to active repository peers",
              },
            ]
          : []),
        ...recipients.map((value) => ({ kind: "actor" as const, recipient: value })),
      ];
      const selected = new Set<string>();
      for (const initialTarget of initialTargets) {
        const normalized = initialTarget.replace(/^@/, "");
        const match = recipients.find((candidate) => candidate.address === normalized || candidate.label.replace(/^@/, "") === normalized);
        selected.add(match?.address ?? normalized);
      }
      let phase: "recipients" | "compose" = initialTargets.length > 0 ? "compose" : "recipients";
      let cursor = 0;
      let cachedWidth: number | undefined;
      let cachedLines: string[] | undefined;

      const editorTheme: EditorTheme = {
        borderColor: (value) => theme.fg("accent", value),
        selectList: {
          selectedPrefix: (value) => theme.fg("accent", value),
          selectedText: (value) => theme.fg("accent", value),
          description: (value) => theme.fg("muted", value),
          scrollInfo: (value) => theme.fg("dim", value),
          noMatch: (value) => theme.fg("warning", value),
        },
      };
      const editor = new Editor(tui, editorTheme);

      function refresh() {
        cachedWidth = undefined;
        cachedLines = undefined;
        tui.requestRender();
      }

      function currentTargets(): string[] {
        return [...selected];
      }

      function rowSelected(row: Row): boolean {
        if (row.kind === "repo") return repoTargets.length > 0 && repoTargets.every((address) => selected.has(address));
        return selected.has(row.recipient.address);
      }

      function toggle(row: Row) {
        if (row.kind === "repo") {
          const allSelected = rowSelected(row);
          for (const address of repoTargets) {
            if (allSelected) selected.delete(address);
            else selected.add(address);
          }
        } else if (selected.has(row.recipient.address)) {
          selected.delete(row.recipient.address);
        } else {
          selected.add(row.recipient.address);
        }
        refresh();
      }

      editor.onSubmit = (value) => {
        const body = value.trim();
        const targets = currentTargets();
        if (body && targets.length > 0) done({ targets, body });
      };

      function handleInput(data: string) {
        if (phase === "compose") {
          if (matchesKey(data, Key.escape)) {
            if (initialTargets.length > 0) done(null);
            else {
              phase = "recipients";
              refresh();
            }
            return;
          }
          editor.handleInput(data);
          refresh();
          return;
        }
        if (matchesKey(data, Key.escape)) {
          done(null);
          return;
        }
        if (matchesKey(data, Key.up) || data === "k") {
          cursor = Math.max(0, cursor - 1);
          refresh();
          return;
        }
        if (matchesKey(data, Key.down) || data === "j") {
          cursor = Math.min(rows.length - 1, cursor + 1);
          refresh();
          return;
        }
        if (matchesKey(data, Key.space)) {
          const row = rows[cursor];
          if (row) toggle(row);
          return;
        }
        if (matchesKey(data, Key.enter)) {
          const row = rows[cursor];
          if (selected.size === 0 && row) toggle(row);
          if (selected.size > 0) {
            phase = "compose";
            refresh();
          }
        }
      }

      function selectedLabel(): string {
        const labels = currentTargets().map(
          (address) => recipients.find((candidate) => candidate.address === address)?.label ?? `@${address}`,
        );
        if (labels.length <= 3) return labels.join(", ");
        return `${labels.slice(0, 3).join(", ")} +${labels.length - 3}`;
      }

      function render(width: number): string[] {
        if (cachedLines && cachedWidth === width) return cachedLines;
        if (width <= 0) return [];
        if (width < 3) return [truncateToWidth("Bus talk", width, "")];
        const renderWidth = width;
        const innerWidth = renderWidth - 2;
        const border = (value: string) => theme.fg("border", value);
        const row = (value = "") => {
          const clipped = truncateToWidth(value, innerWidth, "…");
          return border("│") + clipped + " ".repeat(Math.max(0, innerWidth - visibleWidth(clipped))) + border("│");
        };
        const lines = [border(`╭${"─".repeat(innerWidth)}╮`), row(` ${theme.fg("accent", theme.bold("Bus talk"))}`), row("")];
        if (phase === "recipients") {
          lines.push(row(` ${theme.fg("muted", `Recipients · ${selected.size} selected`)}`));
          const visibleCount = Math.min(10, rows.length);
          const first = Math.min(Math.max(0, cursor - Math.floor(visibleCount / 2)), Math.max(0, rows.length - visibleCount));
          for (let index = first; index < first + visibleCount; index += 1) {
            const item = rows[index];
            if (!item) continue;
            const focused = index === cursor;
            const checked = rowSelected(item) ? theme.fg("success", "■") : theme.fg("dim", "□");
            const marker = focused ? theme.fg("accent", "›") : " ";
            const label = item.kind === "repo" ? item.label : item.recipient.label;
            const description = item.kind === "repo" ? item.description : item.recipient.description;
            lines.push(row(` ${marker} ${checked} ${focused ? theme.fg("accent", label) : label}`));
            lines.push(row(`       ${theme.fg("muted", description)}`));
          }
          lines.push(row(""), row(` ${theme.fg("dim", "↑↓/j/k navigate · Space toggle · Enter compose · Esc cancel")}`));
        } else {
          lines.push(row(` ${theme.fg("muted", "To:")} ${theme.fg("accent", selectedLabel())}`), row(""));
          for (const line of editor.render(Math.max(1, innerWidth - 2))) lines.push(row(` ${line}`));
          lines.push(row(""), row(` ${theme.fg("dim", "Enter send · Shift+Enter newline · Esc recipients/cancel")}`));
        }
        lines.push(border(`╰${"─".repeat(innerWidth)}╯`));
        cachedWidth = width;
        cachedLines = lines;
        return lines;
      }

      return {
        get focused() {
          return editor.focused;
        },
        set focused(value: boolean) {
          editor.focused = value;
        },
        render,
        handleInput,
        invalidate() {
          cachedWidth = undefined;
          cachedLines = undefined;
          editor.invalidate();
        },
      };
    },
    {
      overlay: true,
      overlayOptions: { anchor: "center", width: "72%", minWidth: 70, maxHeight: "82%", margin: 2 },
    },
  );
}
