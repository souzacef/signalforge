# SignalForge frontend

This Angular application is the SignalForge operator console. It provides
authentication and session restoration, an Operations Overview, Incident list
and detail workflows, deterministic triage with advisory AI enrichment, and a
Remediation workspace. The remediation flow supports proposal creation,
human-in-the-loop approval or rejection, controlled admin execution requests,
and durable execution status.

## Development

Use Node.js 24.15.0 or later in the 24.x line and npm 11.13.0 or later in the
11.x line. With the SignalForge backend running at
<http://127.0.0.1:8000>, install and start the frontend:

```sh
npm ci
npm start
```

Open <http://localhost:4200>. The Angular development server uses
`proxy.conf.json` to send relative `/api/...` requests to the local backend.

Run the frontend test suite and production build with:

```sh
npm run test:ci
npm run build
```

The production browser assets are written to `dist/frontend/browser`.

## Production container

`Dockerfile` performs the locked Angular production build in Node and copies
only the browser assets and `nginx.conf` into a pinned Nginx Alpine runtime.
Build it from the repository root with:

```sh
podman build -t signalforge-frontend:local frontend
```

The container listens on port 8080. Nginx serves the Angular application with
SPA fallback, proxies same-origin `/api/...` requests to `backend:8000`, and
serves frontend liveness at `GET /healthz` without contacting the backend. The
root `compose.yaml` supplies the backend network and exposes the packaged
frontend through the optional `demo` profile.
