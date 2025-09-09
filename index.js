// index.js
// Entry point wrapper – calls the orchestrator in migrate.js

import migrate from "./migrate.js";

(async () => {
  try {
    console.log("[index] starting migration…");
    await migrate();
    console.log("[index] finished ✅");
  } catch (err) {
    console.error("[index] Fatal error:", err);
    process.exit(1);
  }
})();
