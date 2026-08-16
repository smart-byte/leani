import { createLeaniClient } from "@leani/sdk";

interface BlockSummary {
  blockNumber: number;
  timestamp: number;
}

const leani = createLeaniClient({ baseUrl: "http://127.0.0.1:9080" });

// docs:start custom-query-client
const capabilities = await leani.capabilities();
const extension = capabilities.queryExtensions.find(
  (candidate) => candidate.processor === "example-block-summary-local",
);
if (!extension) throw new Error("block summary query extension is unavailable");

const summary = await leani.request<BlockSummary>(
  `${extension.basePath}/19426589`,
);
console.log(summary);
// docs:end custom-query-client
