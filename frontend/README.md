# SignalForge frontend

This Angular application is the Phase 6a operator-console foundation. It
implements authentication, session restoration, protected navigation, the app
shell, and honest placeholder routes for later operational feature slices.

Use Node.js 24.15.0 or later in the 24.x line and npm 11.13.0 or later in the
11.x line. With the SignalForge backend running at
<http://127.0.0.1:8000>, install and start the frontend:

```sh
npm ci
npm start
```

Open <http://localhost:4200>. The Angular development server automatically uses
`proxy.conf.json` to send relative `/api` requests to the local backend.

Run the official Angular CLI test target and production build with:

```sh
npm run test:ci
npm run build
```
