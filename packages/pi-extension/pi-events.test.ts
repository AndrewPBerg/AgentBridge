import { isBashToolResult } from "@earendil-works/pi-coding-agent";
import { describe, expect, it } from "vitest";

describe("documented Pi bash event shapes", () => {
  it("narrows a tool_result using its matching tool_call id", () => {
    const toolCall = {
      type: "tool_call",
      toolName: "bash",
      toolCallId: "call-1",
      input: { command: "pnpm test" },
    };
    const toolResult = {
      type: "tool_result",
      toolName: toolCall.toolName,
      toolCallId: toolCall.toolCallId,
      input: toolCall.input,
      content: [{ type: "text" as const, text: "ok\nexit code: 0" }],
      details: { truncation: null, fullOutputPath: null },
      isError: false,
    };

    expect(isBashToolResult(toolResult)).toBe(true);
    if (isBashToolResult(toolResult)) {
      expect(toolResult.details.truncation).toBeNull();
      expect(toolResult.toolCallId).toBe(toolCall.toolCallId);
    }
  });
});
