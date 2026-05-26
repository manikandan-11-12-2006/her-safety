"use strict";

const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  },
  pingTimeout: 60000,
  pingInterval: 25000
});

const PORT = process.env.PORT || 3000;

/* =========================
   STATIC FILES
========================= */

app.use(express.static(path.join(__dirname, "public")));

/* =========================
   ROUTES
========================= */

app.get("/", (_req, res) => {
  res.send("HER ALERT SERVER RUNNING");
});

app.get("/camera1", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "camera1.html"));
});

app.get("/camera2", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "camera2.html"));
});

app.get("/dashboard", (_req, res) => {
  res.sendFile(path.join(__dirname, "public", "police.html"));
});

/* =========================
   STORAGE
========================= */

const cameraRegistry = {};
const dashboardSockets = new Set();
const socketToNode = {};

/* =========================
   HELPERS
========================= */

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

/* =========================
   SOCKET CONNECTION
========================= */

io.on("connection", (socket) => {

  console.log(`[${ts()}] CONNECTED : ${socket.id}`);

  /* CAMERA REGISTER */

  socket.on("register-camera", ({
    nodeId,
    location = "UNKNOWN"
  }) => {

    nodeId = String(nodeId);

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
      location
    });

    broadcastCameraList();
  });

  /* DASHBOARD REGISTER */

  socket.on("register-dashboard", () => {

    dashboardSockets.add(socket.id);

    socket.join("dashboard-room");

    console.log(`[${ts()}] DASHBOARD CONNECTED`);

    socket.emit(
      "camera-list",
      Object.values(cameraRegistry)
    );

  });

  /* OFFER */

  socket.on("offer", (data) => {

    socket.to("dashboard-room").emit("offer", data);

  });

  /* ANSWER */

  socket.on("answer", ({
    nodeId,
    answer
  }) => {

    if (cameraRegistry[nodeId]) {

      io.to(cameraRegistry[nodeId].socketId)
        .emit("answer", {
          nodeId,
          answer
        });

    }

  });

  /* ICE */

  socket.on("ice-candidate", ({
    nodeId,
    candidate,
    target
  }) => {

    if (target === "dashboard") {

      socket.to("dashboard-room")
        .emit("ice-candidate", {
          nodeId,
          candidate
        });

    } else if (
      target === "camera" &&
      cameraRegistry[nodeId]
    ) {

      io.to(cameraRegistry[nodeId].socketId)
        .emit("ice-candidate", {
          nodeId,
          candidate
        });

    }

  });

  /* ALERT */

  socket.on("alert-triggered", (payload) => {

    console.log(`[${ts()}] ALERT FROM ${payload.nodeId}`);

    notifyDashboards("alert-triggered", payload);

  });

  /* DISCONNECT */

  socket.on("disconnect", () => {

    if (socketToNode[socket.id]) {

      const nodeId = socketToNode[socket.id];

      delete cameraRegistry[nodeId];
      delete socketToNode[socket.id];

      notifyDashboards("camera-disconnected", {
        nodeId
      });

      broadcastCameraList();

      console.log(`[${ts()}] CAMERA ${nodeId} DISCONNECTED`);

    }

    if (dashboardSockets.has(socket.id)) {

      dashboardSockets.delete(socket.id);

      console.log(`[${ts()}] DASHBOARD DISCONNECTED`);

    }

  });

});

/* =========================
   START SERVER
========================= */

server.listen(PORT, () => {

  console.log("");
  console.log("==================================");
  console.log("HER ALERT SERVER RUNNING");
  console.log(`PORT : ${PORT}`);
  console.log("==================================");
  console.log("");

  console.log(`/camera1`);
  console.log(`/camera2`);
  console.log(`/dashboard`);

});

/* =========================
   EXPORT
========================= */

module.exports = {
  app,
  server,
  io
};