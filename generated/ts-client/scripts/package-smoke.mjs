import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";

const packageRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
const scratchParent = process.env.TMPDIR ?? tmpdir();
const scratch = await mkdtemp(join(scratchParent, "iris-client-smoke-"));

function run(command, args, cwd, env = process.env) {
  const result = spawnSync(command, args, {
    cwd,
    env,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
  });
  if (result.status !== 0) {
    throw new Error(
      `${command} ${args.join(" ")} failed with ${result.status}:\n${result.stdout}\n${result.stderr}`,
    );
  }
  return result.stdout;
}

try {
  run("npm", ["run", "build"], packageRoot);
  const packJson = run(
    "npm",
    ["pack", "--json", "--ignore-scripts", "--pack-destination", scratch],
    packageRoot,
  );
  const packResult = JSON.parse(packJson);
  const tarball = join(scratch, packResult[0].filename);
  const consumer = join(scratch, "consumer");
  const dependencyPath = relative(consumer, tarball).replaceAll("\\", "/");
  await mkdir(consumer, { recursive: true });

  await writeFile(
    join(consumer, "package.json"),
    JSON.stringify(
      {
        name: "iris-client-smoke-consumer",
        private: true,
        type: "module",
        dependencies: {
          "@techgodhq/iris-client": `file:${dependencyPath}`,
        },
      },
      null,
      2,
    ),
  );
  await writeFile(
    join(consumer, "tsconfig.json"),
    JSON.stringify(
      {
        compilerOptions: {
          target: "ES2022",
          module: "NodeNext",
          moduleResolution: "NodeNext",
          strict: true,
          skipLibCheck: true,
          outDir: "dist",
          lib: ["ES2022", "DOM"],
        },
        include: ["smoke.ts"],
      },
      null,
      2,
    ),
  );
  await writeFile(
    join(consumer, "smoke.ts"),
    `import { IrisClient, type ListThreadsResult } from "@techgodhq/iris-client";\n\nconst client = new IrisClient({ baseUrl: "http://127.0.0.1:9876" });\nconst typedCall = (value: IrisClient): Promise<ListThreadsResult> =>\n  value.listThreads({ limit: 1 });\nvoid typedCall;\nvoid client;\n`,
  );

  run("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund"], consumer);
  const tsc = join(
    packageRoot,
    "node_modules",
    ".bin",
    process.platform === "win32" ? "tsc.cmd" : "tsc",
  );
  run(tsc, ["--project", "tsconfig.json", "--pretty", "false"], consumer);

  const runtimeProbe = `
const pkg = await import("@techgodhq/iris-client");
if (typeof pkg.IrisClient !== "function") throw new Error("IrisClient export missing");
const baseUrl = process.env.IRIS_URL;
if (!baseUrl) {
  console.log(JSON.stringify({ packageImport: "ok", liveSmoke: "skipped" }));
} else {
  const health = await fetch(new URL("/health", baseUrl));
  if (!health.ok) throw new Error(\`health returned \${health.status}\`);
  const client = new pkg.IrisClient({ baseUrl, token: process.env.IRIS_API_TOKEN });
  const threads = await client.listThreads({ limit: 1 });
  if (!Array.isArray(threads)) throw new Error("listThreads did not return an array");
  console.log(JSON.stringify({ packageImport: "ok", healthStatus: health.status, threadCount: threads.length }));
}
`;
  const probeOutput = run(
    process.execPath,
    ["--input-type=module", "-e", runtimeProbe],
    consumer,
  );
  process.stdout.write(probeOutput);
} finally {
  await rm(scratch, { recursive: true, force: true });
}
