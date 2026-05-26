"use strict";

// =============================================================
// HER ALERT – AI WOMEN SAFETY SURVEILLANCE SYSTEM
// server.js  –  Central relay: WebRTC signaling + AI alerts
// FIX: Consistent event names, robust registration, full logging
// =============================================================

const express = require("express");
const http    = require("http");
const { Server } = require("socket.io");
const path    = require("path");

const app    = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin:  "*",
    methods: ["GET", "POST"]
  },
  pingTimeout:  60000,
  pingInterval: 25000,
  // Allow polling fallback for networks that block WebSockets
  transports: ["websocket", "polling"]
});

const PORT = process.env.PORT || 3000;

/* ─────────────────────────────────────────────────────────────
   STATIC FILES  (camera1.html, camera2.html, police.html
   must live inside /public)
───────────────────────────────────────────────────────────── */
app.use(express.static(path.join(__dirname, "public")));

/* ─────────────────────────────────────────────────────────────
   ROUTES
───────────────────────────────────────────────────────────── */
app.get("/",          (_req, res) => res.send("HER ALERT SERVER RUNNING — open /camera1, /camera2, or /dashboard"));
app.get("/camera1",   (_req, res) => res.sendFile(path.join(__dirname, "public", "camera1.html")));
app.get("/camera2",   (_req, res) => res.sendFile(path.join(__dirname, "public", "camera2.html")));
app.get("/dashboard", (_req, res) => res.sendFile(path.join(__dirname, "public", "police.html")));
app.get("/police",    (_req, res) => res.sendFile(path.join(__dirname, "public", "police.html")));

/* ─────────────────────────────────────────────────────────────
   IN-MEMORY STATE
───────────────────────────────────────────────────────────── */
// nodeId → { socketId, nodeId, connectedAt, location }
const cameraRegistry  = {};
// Set of dashboard socket IDs
const dashboardSockets = new Set();
// socket.id → nodeId  (for disconnect cleanup)
const socketToNode = {};

/* ─────────────────────────────────────────────────────────────
   HELPERS
───────────────────────────────────────────────────────────── */
function ts() {
  return new Date().toLocaleTimeString("en-US", { hour12: false });
}

/** Push live camera list to every dashboard */
function broadcastCameraList() {
  const list = Object.values(cameraRegistry).map(cam => ({
    nodeId:      cam.nodeId,
    socketId:    cam.socketId,
    connectedAt: cam.connectedAt,
    location:    cam.location,
    active:      true
  }));
  for (const id of dashboardSockets) {
    io.to(id).emit("camera-list", list);
  }
}

/** Send any event + payload to every dashboard */
function notifyDashboards(event, payload) {
  for (const id of dashboardSockets) {
    io.to(id).emit(event, payload);
  }
  console.log(`[${ts()}] → DASHBOARDS (${dashboardSockets.size}) : ${event}`);
}

/* ─────────────────────────────────────────────────────────────
   SOCKET LOGIC
───────────────────────────────────────────────────────────── */
io.on("connection", socket => {

  console.log(`[${ts()}] CONNECTED   : ${socket.id}`);

  /* ── CAMERA REGISTERS ──────────────────────────────────── */
  socket.on("register-camera", ({ nodeId, location = "UNKNOWN" }) => {
    nodeId = String(nodeId);

    cameraRegistry[nodeId] = {
      socketId:    socket.id,
      nodeId,
      connectedAt: new Date().toISOString(),
      location
    };
    socketToNode[socket.id] = nodeId;

    socket.join(`camera-${nodeId}`);
    console.log(`[${ts()}] CAMERA ${nodeId} REGISTERED  loc="${location}"  sid=${socket.id}`);

    notifyDashboards("camera-connected", { nodeId, socketId: socket.id, location });
    broadcastCameraList();

    // Confirm back to camera
    socket.emit("register-ack", { nodeId, status: "ok" });
  });

  /* ── DASHBOARD REGISTERS ───────────────────────────────── */
  socket.on("register-dashboard", () => {
    dashboardSockets.add(socket.id);
    socket.join("dashboard-room");
    console.log(`[${ts()}] DASHBOARD REGISTERED  sid=${socket.id}  (total=${dashboardSockets.size})`);

    // Send current camera list immediately
    socket.emit("camera-list", Object.values(cameraRegistry));
    // Confirm back to dashboard
    socket.emit("register-ack", { role: "dashboard", status: "ok" });
  });

  /* ── WebRTC SIGNALING ──────────────────────────────────── */
  socket.on("offer", data => {
    console.log(`[${ts()}] OFFER  nodeId=${data.nodeId} → dashboards`);
    socket.to("dashboard-room").emit("offer", data);
  });

  socket.on("answer", ({ nodeId, answer }) => {
    const cam = cameraRegistry[String(nodeId)];
    console.log(`[${ts()}] ANSWER  nodeId=${nodeId} → camera socketId=${cam?.socketId}`);
    if (cam) io.to(cam.socketId).emit("answer", { nodeId, answer });
  });

  socket.on("ice-candidate", ({ nodeId, candidate, target }) => {
    if (target === "dashboard") {
      socket.to("dashboard-room").emit("ice-candidate", { nodeId, candidate });
    } else if (target === "camera") {
      const cam = cameraRegistry[String(nodeId)];
      if (cam) io.to(cam.socketId).emit("ice-candidate", { nodeId, candidate });
    }
  });

  /* ── AI ALERT RELAY ────────────────────────────────────── */
  /**
   * THE CRITICAL FIX:
   * Cameras emit  "alert-triggered"
   * Server relays "alert-triggered"  to ALL dashboards
   * Dashboard listens for "alert-triggered"
   *
   * Also handle legacy event names just in case.
   *
   * Payload:
   * {
   *   nodeId      : "NODE_1" | "NODE_2"
   *   alertType   : "VOICE" | "SCREAM" | "POSE"
   *   word        : string
   *   level       : number (0-100)
   *   description : string
   *   timestamp   : string
   *   location    : string
   * }
   */
  function relayAlert(payload) {
    const safePayload = {
      nodeId:      payload.nodeId      || "UNKNOWN",
      alertType:   payload.alertType   || payload.type || "ALERT",
      word:        payload.word        || payload.detected || "",
      level:       payload.level       || payload.threatLevel || 100,
      description: payload.description || payload.message || "Emergency detected",
      timestamp:   payload.timestamp   || new Date().toLocaleTimeString("en-US", { hour12: false }),
      location:    payload.location    || cameraRegistry[payload.nodeId]?.location || "UNKNOWN"
    };

    console.log(`[${ts()}] 🚨 ALERT  node=${safePayload.nodeId}  type=${safePayload.alertType}  desc="${safePayload.description}"`);
    notifyDashboards("alert-triggered", safePayload);
  }

  // Primary event (used by all camera nodes)
  socket.on("alert-triggered", relayAlert);

  // Legacy / alternative event names – all relay the same way
  socket.on("alert",           relayAlert);
  socket.on("voice-alert",     relayAlert);
  socket.on("threat-alert",    relayAlert);

  /* ── CAMERA STATUS ─────────────────────────────────────── */
  socket.on("camera-status", (payload) => {
    console.log(`[${ts()}] STATUS  node=${payload.nodeId}  status=${payload.status}`);
    notifyDashboards("camera-status", payload);
  });

  /* ── DISCONNECT ────────────────────────────────────────── */
  socket.on("disconnect", (reason) => {

    const nodeId = socketToNode[socket.id];
    if (nodeId) {
      delete cameraRegistry[nodeId];
      delete socketToNode[socket.id];
      notifyDashboards("camera-disconnected", { nodeId });
      broadcastCameraList();
      console.log(`[${ts()}] CAMERA ${nodeId} DISCONNECTED  reason=${reason}`);
    }

    if (dashboardSockets.has(socket.id)) {
      dashboardSockets.delete(socket.id);
      console.log(`[${ts()}] DASHBOARD DISCONNECTED  sid=${socket.id}  remaining=${dashboardSockets.size}`);
    }

    if (!nodeId && !dashboardSockets.has(socket.id)) {
      console.log(`[${ts()}] UNKNOWN CLIENT DISCONNECTED  sid=${socket.id}`);
    }
  });
});

/* ─────────────────────────────────────────────────────────────
   START
───────────────────────────────────────────────────────────── */
server.listen(PORT, () => {
  console.log("");
  console.log("══════════════════════════════════════════");
  console.log("  HER ALERT – AI SURVEILLANCE SERVER      ");
  console.log(`  PORT : ${PORT}                           `);
  console.log("══════════════════════════════════════════");
  console.log(`  http://localhost:${PORT}/camera1   → Camera Node 01`);
  console.log(`  http://localhost:${PORT}/camera2   → Camera Node 02`);
  console.log(`  http://localhost:${PORT}/dashboard → Control Dashboard`);
  console.log("══════════════════════════════════════════\n");
});

module.exports = { app, server, io };