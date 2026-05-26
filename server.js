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