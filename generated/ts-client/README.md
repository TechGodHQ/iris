# `@techgodhq/iris-client`

The generated, zero-runtime-dependency TypeScript fetch client for
[Iris](https://github.com/TechGodHQ/iris). The module is projected from Iris's
`api/operations.yaml` by Hydra; do not edit `index.ts` by hand.

## Install

```bash
npm install @techgodhq/iris-client
```

The package version is aligned with the Iris release tag that published it.
The generated client uses the platform `fetch` API and accepts an injectable
fetch implementation for tests or custom runtimes.

## Use

```ts
import { IrisClient } from "@techgodhq/iris-client";

const client = new IrisClient({
  baseUrl: process.env.IRIS_URL ?? "http://127.0.0.1:9876",
  token: process.env.IRIS_API_TOKEN,
});

const threads = await client.listThreads({ limit: 10 });
```

`IrisClient` preserves the operation contract's explicit path, query, and JSON
body locations. Non-2xx responses throw `ApiError` with the HTTP status and
parsed response body. The SSE operation is intentionally excluded: streaming
TypeScript support requires a separate contract rather than pretending an SSE
response is JSON.

## Repository checks

From this directory in the Iris repository:

```bash
npm ci
npm run typecheck
npm run build
npm run package:smoke
```

`package:smoke` packs the package, installs that exact tarball into a temporary
consumer, compiles a typed import, and verifies the runtime import. When
`IRIS_URL` is supplied, it additionally checks `GET /health` and calls
`listThreads` against that target. `IRIS_API_TOKEN` is passed only to the
client request and is never printed.
