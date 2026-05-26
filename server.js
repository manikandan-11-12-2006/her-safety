<<<<<<< HEAD
/**
 * ============================================================
 *  HER ALERT – Real-Time AI Surveillance Server
 * ============================================================
 */

"use strict";

/* ============================================================
   DEPENDENCIES
============================================================ */

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");

/* ============================================================
   APP SETUP
============================================================ */

const app = express();
const server = http.createServer(app);

/* ============================================================
   SOCKET.IO
============================================================ */

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },

  pingTimeout: 60000,
  pingInterval: 25000
});

/* ============================================================
   PORT
============================================================ */

const PORT = process.env.PORT || 3000;

/* ============================================================
   STATIC FILES
============================================================ */

app.use(express.static(path.join(__dirname, "public")));

/* ============================================================
   ROOT ROUTE
============================================================ */

app.get("/", (_req, res) => {
  res.send("HER ALERT SERVER RUNNING");
});

/* ============================================================
   PAGE ROUTES
============================================================ */

app.get("/camera1", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "camera1.html"));
});

app.get("/camera2", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "camera2.html"));
});

app.get("/dashboard", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "police.html"));
});

/* ============================================================
   HEALTH ROUTE
============================================================ */

app.get("/health", (_req, res) => {

  res.json({
    status: "online",
    uptime: process.uptime(),
    cameras: Object.keys(cameraRegistry).length,
    dashboards: dashboardSockets.size,
    timestamp: new Date().toISOString()
  });

});

/* ============================================================
   STATE STORAGE
============================================================ */

const cameraRegistry = {};

const dashboardSockets = new Set();

const socketToNode = {};

/* ============================================================
   HELPER FUNCTIONS
============================================================ */

function ts() {

  return new Date().toLocaleTimeString("en-US", {
    hour12: false
  });

}

function broadcastCameraList() {

  const list = Object.values(cameraRegistry).map((cam) => ({
    nodeId: cam.nodeId,
    socketId: cam.socketId,
    connectedAt: cam.connectedAt,
    location: cam.location,
    active: true
  }));

  for (const dashId of dashboardSockets) {

    io.to(dashId).emit("camera-list", list);

  }

}

function notifyDashboards(event, payload) {

  for (const dashId of dashboardSockets) {

    io.to(dashId).emit(event, payload);

  }

}

/* ============================================================
   SOCKET CONNECTION
============================================================ */

io.on("connection", (socket) => {

  console.log(`[${ts()}] SOCKET CONNECTED : ${socket.id}`);

  /* ============================================================
     REGISTER CAMERA
  ============================================================ */

  socket.on("register-camera", ({
    nodeId,
    location = "UNKNOWN LOCATION"
  }) => {

    nodeId = String(nodeId);

    if (cameraRegistry[nodeId]) {

      const oldId = cameraRegistry[nodeId].socketId;

      delete socketToNode[oldId];

      console.log(`[${ts()}] REPLACED OLD CAMERA ${nodeId}`);

    }

    cameraRegistry[nodeId] = {

      socketId: socket.id,
      nodeId,
      connectedAt: new Date().toISOString(),
      location

    };

    socketToNode[socket.id] = nodeId;

    socket.join(`camera-${nodeId}`);

    console.log(`[${ts()}] CAMERA ${nodeId} CONNECTED`);

    notifyDashboards("camera-connected", {

      nodeId,
      socketId: socket.id,
      location,
      connectedAt: cameraRegistry[nodeId].connectedAt

    });

    socket.emit("dashboard-count", {

      count: dashboardSockets.size

    });

    broadcastCameraList();

  });

  /* ============================================================
     REGISTER DASHBOARD
  ============================================================ */

  socket.on("register-dashboard", () => {

    dashboardSockets.add(socket.id);

    socket.join("dashboard-room");

    console.log(`[${ts()}] DASHBOARD CONNECTED`);

    const activeCams = Object.values(cameraRegistry).map((cam) => ({

      nodeId: cam.nodeId,
      socketId: cam.socketId,
      connectedAt: cam.connectedAt,
      location: cam.location,
      active: true

    }));

    socket.emit("camera-list", activeCams);

    socket.emit("dashboard-registered", {

      socketId: socket.id,
      activeCams: activeCams.length,
      serverTime: new Date().toISOString()

    });

    for (const [nodeId] of Object.entries(cameraRegistry)) {

      io.to(`camera-${nodeId}`).emit("dashboard-joined", {

        dashboardSocketId: socket.id

      });

    }

  });

  /* ============================================================
     OFFER
  ============================================================ */

  socket.on("offer", ({
    nodeId,
    offer,
    targetDashboardId
  }) => {

    nodeId = String(nodeId);

    if (targetDashboardId) {

      io.to(targetDashboardId).emit("offer", {

        nodeId,
        fromSocketId: socket.id,
        offer

      });

    } else {

      socket.to("dashboard-room").emit("offer", {

        nodeId,
        fromSocketId: socket.id,
        offer

      });

    }

    console.log(`[${ts()}] OFFER FROM CAMERA ${nodeId}`);

  });

  /* ============================================================
     ANSWER
  ============================================================ */

  socket.on("answer", ({
    nodeId,
    answer,
    targetCameraSocketId
  }) => {

    nodeId = String(nodeId);

    if (targetCameraSocketId) {

      io.to(targetCameraSocketId).emit("answer", {

        fromDashboardId: socket.id,
        nodeId,
        answer

      });

    } else if (cameraRegistry[nodeId]) {

      io.to(cameraRegistry[nodeId].socketId).emit("answer", {

        fromDashboardId: socket.id,
        nodeId,
        answer

      });

    }

    console.log(`[${ts()}] ANSWER SENT TO CAMERA ${nodeId}`);

  });

  /* ============================================================
     ICE CANDIDATE
  ============================================================ */

  socket.on("ice-candidate", ({
    nodeId,
    candidate,
    target,
    targetSocketId
  }) => {

    nodeId = String(nodeId);

    if (target === "dashboard") {

      if (targetSocketId) {

        io.to(targetSocketId).emit("ice-candidate", {

          nodeId,
          fromSocketId: socket.id,
          candidate

        });

      } else {

        socket.to("dashboard-room").emit("ice-candidate", {

          nodeId,
          fromSocketId: socket.id,
          candidate

        });

      }

    }

    else if (target === "camera") {

      if (targetSocketId) {

        io.to(targetSocketId).emit("ice-candidate", {

          nodeId,
          fromSocketId: socket.id,
          candidate

        });

      }

      else if (cameraRegistry[nodeId]) {

        io.to(cameraRegistry[nodeId].socketId).emit("ice-candidate", {

          nodeId,
          fromSocketId: socket.id,
          candidate

        });

      }

    }

  });

  /* ============================================================
     ALERTS
  ============================================================ */

  socket.on("alert-triggered", (payload) => {

    const nodeId = String(payload.nodeId || "?");

    console.log(`[${ts()}] ALERT FROM CAMERA ${nodeId}`);

    notifyDashboards("alert-triggered", {

      ...payload,
      nodeId,
      receivedAt: new Date().toISOString()

    });

  });

  /* ============================================================
     MOTION UPDATE
  ============================================================ */

  socket.on("motion-update", (payload) => {

    const nodeId = String(payload.nodeId || "?");

    notifyDashboards("motion-update", {

      ...payload,
      nodeId,
      timestamp: Date.now()

    });

  });

  /* ============================================================
     CAMERA READY
  ============================================================ */

  socket.on("camera-ready", ({
    nodeId
  }) => {

    nodeId = String(nodeId);

    console.log(`[${ts()}] CAMERA READY ${nodeId}`);

    for (const dashId of dashboardSockets) {

      socket.emit("dashboard-joined", {

        dashboardSocketId: dashId

      });

    }

  });

  /* ============================================================
     NODE STATUS
  ============================================================ */

  socket.on("node-status", (payload) => {

    const nodeId = String(payload.nodeId || "?");

    notifyDashboards("node-status", {

      ...payload,
      nodeId

    });

  });

  /* ============================================================
     DISCONNECT
  ============================================================ */

  socket.on("disconnect", (reason) => {

    if (socketToNode[socket.id]) {

      const nodeId = socketToNode[socket.id];

      delete cameraRegistry[nodeId];

      delete socketToNode[socket.id];

      console.log(`[${ts()}] CAMERA ${nodeId} DISCONNECTED`);

      notifyDashboards("camera-disconnected", {

        nodeId,
        reason,
        disconnectedAt: new Date().toISOString()

      });

      broadcastCameraList();

    }

    else if (dashboardSockets.has(socket.id)) {

      dashboardSockets.delete(socket.id);

      console.log(`[${ts()}] DASHBOARD DISCONNECTED`);

    }

    else {

      console.log(`[${ts()}] UNKNOWN SOCKET DISCONNECTED`);

    }

  });

  /* ============================================================
     SOCKET ERROR
  ============================================================ */

  socket.on("error", (err) => {

    console.error(`[${ts()}] SOCKET ERROR : ${err.message}`);

  });

});

/* ============================================================
   SERVER ERROR
============================================================ */

server.on("error", (err) => {

  console.error(err);

});

/* ============================================================
   START SERVER
============================================================ */

server.listen(PORT, () => {

  console.log("");
  console.log("==============================================");
  console.log("HER ALERT SERVER RUNNING");
  console.log(`PORT : ${PORT}`);
  console.log("==============================================");
  console.log("");
  console.log(`CAMERA 1  : /camera1`);
  console.log(`CAMERA 2  : /camera2`);
  console.log(`DASHBOARD : /dashboard`);
  console.log(`HEALTH    : /health`);
  console.log("");

});

/* ============================================================
   GRACEFUL SHUTDOWN
============================================================ */

function shutdown(signal) {

  console.log(`[${ts()}] SHUTTING DOWN : ${signal}`);

  io.emit("server-shutdown", {

    reason: signal

  });

  server.close(() => {

    process.exit(0);

  });

}

process.on("SIGINT", () => shutdown("SIGINT"));

process.on("SIGTERM", () => shutdown("SIGTERM"));

/* ============================================================
   EXPORTS
============================================================ */

module.exports = {

  app,
  server,
  io

};
=======
/**
 * ============================================================
 *  HER ALERT – Real-Time AI Surveillance Server
 *  Project  : HER ALERT – Women Safety Surveillance System
 *  Stack    : Node.js · Express · Socket.io · WebRTC Signaling
 *  Nodes    : Camera Node 1 · Camera Node 2 · Police Dashboard
 *  Author   : HER ALERT Dev Team
 *  Version  : 2.4.1
 * ============================================================
 */

"use strict";

// ── Core dependencies ────────────────────────────────────────
const express = require("express");
const http    = require("http");
const { Server } = require("socket.io");
const path    = require("path");

// ── App bootstrap ────────────────────────────────────────────
const app    = express();
const server = http.createServer(app);

// ── Socket.io server ─────────────────────────────────────────
const io = new Server(server, {
  cors: {
    origin: "*",           // Allow any origin (tighten in production)
    methods: ["GET", "POST"],
  },
  // Increase ping tolerances so laptops on Wi-Fi don't drop early
  pingTimeout:  60000,
  pingInterval: 25000,
});

// ── Configuration ────────────────────────────────────────────
const PORT = 3000;

// ── Static file serving ──────────────────────────────────────
// Place camera1.html, camera2.html, police.html inside ./public/
app.use(express.static(path.join(__dirname, "public")));

app.get("/camera1", (req, res) => {
    res.sendFile(__dirname + "/public/camera1.html");
});

app.get("/camera2", (req, res) => {
    res.sendFile(__dirname + "/public/camera2.html");
});

app.get("/dashboard", (req, res) => {
    res.sendFile(__dirname + "/public/police.html");
});

// ── Convenience routes ───────────────────────────────────────
app.get("/camera1",  (_req, res) => res.sendFile(path.join(__dirname, "public", "camera1.html")));
app.get("/camera2",  (_req, res) => res.sendFile(path.join(__dirname, "public", "camera2.html")));
app.get("/dashboard",(_req, res) => res.sendFile(path.join(__dirname, "public", "police.html")));

// Health check endpoint for uptime monitoring
app.get("/health", (_req, res) => {
  res.json({
    status   : "online",
    uptime   : process.uptime(),
    cameras  : Object.keys(cameraRegistry).length,
    dashboards: dashboardSockets.size,
    timestamp: new Date().toISOString(),
  });
});

// ── State registries ─────────────────────────────────────────
/**
 * cameraRegistry – tracks connected camera nodes
 * Key  : nodeId  (e.g. "1" or "2")
 * Value: { socketId, nodeId, connectedAt, location }
 */
const cameraRegistry = {};

/**
 * dashboardSockets – Set of socket IDs that belong to dashboard clients
 */
const dashboardSockets = new Set();

/**
 * socketToNode – reverse lookup: socketId → nodeId
 * Used quickly on disconnect to know which node left.
 */
const socketToNode = {};

// ── Helpers ──────────────────────────────────────────────────

/** Timestamp prefix for console output */
function ts() {
  return new Date().toLocaleTimeString("en-US", { hour12: false });
}

/** Emit the current active-camera list to all dashboards */
function broadcastCameraList() {
  const list = Object.values(cameraRegistry).map((cam) => ({
    nodeId     : cam.nodeId,
    socketId   : cam.socketId,
    connectedAt: cam.connectedAt,
    location   : cam.location,
    active     : true,
  }));

  for (const dashId of dashboardSockets) {
    io.to(dashId).emit("camera-list", list);
  }
}

/** Notify all dashboards that a camera node changed status */
function notifyDashboards(event, payload) {
  for (const dashId of dashboardSockets) {
    io.to(dashId).emit(event, payload);
  }
}

// ── Socket.io connection handler ─────────────────────────────
io.on("connection", (socket) => {

  console.log(`[${ts()}] [SOCKET CONNECTED] id=${socket.id}`);

  // ──────────────────────────────────────────────────────────
  // EVENT: register-camera
  // Sent by a camera laptop when it loads camera1.html / camera2.html.
  // Payload: { nodeId: "1" | "2", location: string }
  // ──────────────────────────────────────────────────────────
  socket.on("register-camera", ({ nodeId, location = "UNKNOWN LOCATION" }) => {

    nodeId = String(nodeId); // normalise to string

    // If a previous socket was registered for this nodeId, clean it up
    if (cameraRegistry[nodeId]) {
      const oldId = cameraRegistry[nodeId].socketId;
      delete socketToNode[oldId];
      console.log(`[${ts()}] [RE-REGISTER] Camera Node ${nodeId} replaced old socket ${oldId}`);
    }

    // Register the new socket
    cameraRegistry[nodeId] = {
      socketId   : socket.id,
      nodeId,
      connectedAt: new Date().toISOString(),
      location,
    };
    socketToNode[socket.id] = nodeId;

    // Join a named room so the dashboard can address it directly
    socket.join(`camera-${nodeId}`);

    console.log(`[${ts()}] [CONNECTED] Camera Node ${nodeId} | socket=${socket.id} | ${location}`);

    // Tell ALL dashboards about the new camera
    notifyDashboards("camera-connected", {
      nodeId,
      socketId   : socket.id,
      location,
      connectedAt: cameraRegistry[nodeId].connectedAt,
    });

    // Send the camera the current list of online dashboards (so it
    // can immediately begin the WebRTC offer when a dashboard is ready)
    socket.emit("dashboard-count", { count: dashboardSockets.size });

    // Push updated full camera list to every dashboard
    broadcastCameraList();
  });

  // ──────────────────────────────────────────────────────────
  // EVENT: register-dashboard
  // Sent by the police laptop when police.html loads.
  // Payload: none (or optional { operatorId })
  // ──────────────────────────────────────────────────────────
  socket.on("register-dashboard", (data = {}) => {

    dashboardSockets.add(socket.id);
    socket.join("dashboard-room");

    console.log(`[${ts()}] [CONNECTED] Dashboard | socket=${socket.id}`);

    // Immediately tell the dashboard which cameras are already online
    const activeCams = Object.values(cameraRegistry).map((cam) => ({
      nodeId     : cam.nodeId,
      socketId   : cam.socketId,
      connectedAt: cam.connectedAt,
      location   : cam.location,
      active     : true,
    }));

    socket.emit("camera-list", activeCams);
    socket.emit("dashboard-registered", {
      socketId  : socket.id,
      activeCams: activeCams.length,
      serverTime: new Date().toISOString(),
    });

    // Let camera nodes know a dashboard just joined (triggers offer)
    for (const [nodeId] of Object.entries(cameraRegistry)) {
      io.to(`camera-${nodeId}`).emit("dashboard-joined", {
        dashboardSocketId: socket.id,
      });
    }

    console.log(
      `[${ts()}] [DASHBOARD] Notified of ${activeCams.length} active camera(s)`
    );
  });

  // ──────────────────────────────────────────────────────────
  // WebRTC SIGNALING — OFFER
  // Sent by a camera node TO the server; routed to ALL dashboards.
  // Payload: { nodeId, offer: RTCSessionDescriptionInit, targetDashboardId? }
  // ──────────────────────────────────────────────────────────
  socket.on("offer", ({ nodeId, offer, targetDashboardId }) => {

    nodeId = String(nodeId);

    if (targetDashboardId) {
      // Route to a specific dashboard socket
      io.to(targetDashboardId).emit("offer", {
        nodeId,
        fromSocketId: socket.id,
        offer,
      });
      console.log(
        `[${ts()}] [OFFER] Node ${nodeId} → Dashboard ${targetDashboardId}`
      );
    } else {
      // Broadcast to every dashboard in the room
      socket.to("dashboard-room").emit("offer", {
        nodeId,
        fromSocketId: socket.id,
        offer,
      });
      console.log(
        `[${ts()}] [OFFER] Node ${nodeId} → all dashboards (${dashboardSockets.size})`
      );
    }
  });

  // ──────────────────────────────────────────────────────────
  // WebRTC SIGNALING — ANSWER
  // Sent by the dashboard BACK to a specific camera node.
  // Payload: { nodeId, answer: RTCSessionDescriptionInit, targetCameraSocketId }
  // ──────────────────────────────────────────────────────────
  socket.on("answer", ({ nodeId, answer, targetCameraSocketId }) => {

    nodeId = String(nodeId);

    if (targetCameraSocketId) {
      // Route directly to the camera socket that sent the offer
      io.to(targetCameraSocketId).emit("answer", {
        fromDashboardId: socket.id,
        nodeId,
        answer,
      });
      console.log(
        `[${ts()}] [ANSWER] Dashboard ${socket.id} → Node ${nodeId} (${targetCameraSocketId})`
      );
    } else if (cameraRegistry[nodeId]) {
      // Fallback: look up the camera socket via nodeId
      const camSocketId = cameraRegistry[nodeId].socketId;
      io.to(camSocketId).emit("answer", {
        fromDashboardId: socket.id,
        nodeId,
        answer,
      });
      console.log(
        `[${ts()}] [ANSWER] Dashboard ${socket.id} → Node ${nodeId} via registry`
      );
    } else {
      console.warn(
        `[${ts()}] [ANSWER] Could not route answer – Node ${nodeId} not found`
      );
    }
  });

  // ──────────────────────────────────────────────────────────
  // WebRTC SIGNALING — ICE CANDIDATE
  // Relayed in both directions (camera ↔ dashboard).
  // Payload: { nodeId, candidate: RTCIceCandidateInit, target: "dashboard" | "camera", targetSocketId? }
  // ──────────────────────────────────────────────────────────
  socket.on("ice-candidate", ({ nodeId, candidate, target, targetSocketId }) => {

    nodeId = String(nodeId);

    if (target === "dashboard") {
      // Camera is sending ICE candidate to the dashboard(s)
      if (targetSocketId) {
        io.to(targetSocketId).emit("ice-candidate", {
          nodeId,
          fromSocketId: socket.id,
          candidate,
        });
      } else {
        socket.to("dashboard-room").emit("ice-candidate", {
          nodeId,
          fromSocketId: socket.id,
          candidate,
        });
      }
    } else if (target === "camera") {
      // Dashboard is sending ICE candidate back to a camera
      if (targetSocketId) {
        io.to(targetSocketId).emit("ice-candidate", {
          nodeId,
          fromSocketId: socket.id,
          candidate,
        });
      } else if (cameraRegistry[nodeId]) {
        io.to(cameraRegistry[nodeId].socketId).emit("ice-candidate", {
          nodeId,
          fromSocketId: socket.id,
          candidate,
        });
      }
    }

    // (Verbose ICE logging is omitted to keep console clean;
    //  uncomment below for deep debugging)
    // console.log(`[${ts()}] [ICE] Node ${nodeId} | target=${target}`);
  });

  // ──────────────────────────────────────────────────────────
  // EVENT: alert-triggered
  // Camera node broadcasts a suspicious-activity alert.
  // Payload: { nodeId, location, gps, confidence, timestamp }
  // ──────────────────────────────────────────────────────────
  socket.on("alert-triggered", (payload) => {
    const nodeId = String(payload.nodeId || "?");
    console.log(
      `[${ts()}] [ALERT] 🚨 Suspicious activity – Node ${nodeId} | ${payload.location}`
    );

    // Forward the alert to every dashboard
    notifyDashboards("alert-triggered", {
      ...payload,
      nodeId,
      receivedAt: new Date().toISOString(),
    });
  });

  // ──────────────────────────────────────────────────────────
  // EVENT: motion-update
  // Camera nodes periodically push motion & AI metric snapshots
  // so the police dashboard can update its meters in real time.
  // Payload: { nodeId, motionPct, humans, threatLevel, confidence }
  // ──────────────────────────────────────────────────────────
  socket.on("motion-update", (payload) => {
    const nodeId = String(payload.nodeId || "?");

    // Fan out to every connected dashboard
    notifyDashboards("motion-update", {
      ...payload,
      nodeId,
      timestamp: Date.now(),
    });
  });

  // ──────────────────────────────────────────────────────────
  // EVENT: camera-ready
  // Camera sends this after connecting if dashboards already exist.
  // Server responds by sending dashboard-joined to the camera for each active dashboard.
  // ──────────────────────────────────────────────────────────
  socket.on("camera-ready", ({ nodeId }) => {
    nodeId = String(nodeId);
    console.log(`[${ts()}] [CAMERA-READY] Node ${nodeId} requesting dashboard list`);
    // Send dashboard-joined for every connected dashboard
    for (const dashId of dashboardSockets) {
      socket.emit("dashboard-joined", { dashboardSocketId: dashId });
      console.log(`[${ts()}] [CAMERA-READY] Sent dashboard-joined to Node ${nodeId} for dashboard ${dashId}`);
    }
  });

  // ──────────────────────────────────────────────────────────
  // EVENT: node-status
  // Generic status pings from any node (heartbeat).
  // ──────────────────────────────────────────────────────────
  socket.on("node-status", (payload) => {
    const nodeId = String(payload.nodeId || "?");
    // Broadcast to dashboards
    notifyDashboards("node-status", { ...payload, nodeId });
  });

  // ──────────────────────────────────────────────────────────
  // EVENT: disconnect
  // ──────────────────────────────────────────────────────────
  socket.on("disconnect", (reason) => {

    // ---- Was it a camera node? ----
    if (socketToNode[socket.id]) {
      const nodeId = socketToNode[socket.id];
      delete cameraRegistry[nodeId];
      delete socketToNode[socket.id];

      console.log(
        `[${ts()}] [DISCONNECTED] Camera Node ${nodeId} | reason: ${reason}`
      );

      // Notify every dashboard the camera has gone offline
      notifyDashboards("camera-disconnected", {
        nodeId,
        reason,
        disconnectedAt: new Date().toISOString(),
      });

      // Push refreshed (now-shorter) camera list to dashboards
      broadcastCameraList();

    // ---- Was it a dashboard? ----
    } else if (dashboardSockets.has(socket.id)) {
      dashboardSockets.delete(socket.id);

      console.log(
        `[${ts()}] [DISCONNECTED] Dashboard | socket=${socket.id} | reason: ${reason}`
      );

      // Let cameras know a dashboard dropped (they can pause offering)
      for (const [nodeId] of Object.entries(cameraRegistry)) {
        io.to(`camera-${nodeId}`).emit("dashboard-left", {
          dashboardSocketId: socket.id,
          remainingDashboards: dashboardSockets.size,
        });
      }

    // ---- Unknown/unregistered socket ----
    } else {
      console.log(
        `[${ts()}] [DISCONNECTED] Unregistered socket | id=${socket.id} | reason: ${reason}`
      );
    }
  });

  // ──────────────────────────────────────────────────────────
  // EVENT: connect_error (server-side hook for diagnostics)
  // ──────────────────────────────────────────────────────────
  socket.on("error", (err) => {
    console.error(`[${ts()}] [SOCKET ERROR] id=${socket.id} | ${err.message}`);
  });

}); // end io.on("connection")

// ── Server-level error handling ──────────────────────────────
server.on("error", (err) => {
  if (err.code === "EADDRINUSE") {
    console.error(`[ERROR] Port ${PORT} is already in use. Stop the other process and try again.`);
    process.exit(1);
  } else {
    console.error("[SERVER ERROR]", err);
  }
});

// ── Start listening ──────────────────────────────────────────
server.listen(PORT, () => {
  console.log("");
  console.log("╔══════════════════════════════════════════════════════════╗");
  console.log("║                                                          ║");
  console.log("║      HER ALERT  –  Real-Time AI Surveillance Server      ║");
  console.log("║                                                          ║");
  console.log(`║      http://localhost:${PORT}                                ║`);
  console.log("║                                                          ║");
  console.log("║   Routes                                                 ║");
  console.log(`║   /camera1    →  Camera Node 01  (Street Light Area 1)  ║`);
  console.log(`║   /camera2    →  Camera Node 02  (Street Light Area 2)  ║`);
  console.log(`║   /dashboard  →  Police Central Control Dashboard        ║`);
  console.log(`║   /health     →  Server health / status JSON             ║`);
  console.log("║                                                          ║");
  console.log("║   Socket.io events                                       ║");
  console.log("║   register-camera  · register-dashboard                  ║");
  console.log("║   offer  · answer  · ice-candidate                       ║");
  console.log("║   alert-triggered  · motion-update  · disconnect         ║");
  console.log("║                                                          ║");
  console.log("╚══════════════════════════════════════════════════════════╝");
  console.log("");
  console.log(`[${ts()}] Waiting for connections...`);
  console.log("");
});

// ── Graceful shutdown ────────────────────────────────────────
function shutdown(signal) {
  console.log(`\n[${ts()}] [${signal}] Shutting down HER ALERT server...`);
  io.emit("server-shutdown", { reason: signal });
  server.close(() => {
    console.log(`[${ts()}] Server closed cleanly.`);
    process.exit(0);
  });
  // Force-kill after 5 s if connections stall
  setTimeout(() => process.exit(1), 5000).unref();
}

process.on("SIGINT",  () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

// ── Module export (for tests) ────────────────────────────────
module.exports = { app, server, io };
>>>>>>> 173a5b8e3c5b82f1b5796d0aec54d22416e49c0e
