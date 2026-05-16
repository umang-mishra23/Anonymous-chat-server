
import http from "http";
import express from "express";
import { WebSocketServer, WebSocket } from "ws";
import crypto from "crypto";

const app = express();
app.use(express.static("public"));

const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const PORT = process.env.PORT || 3000;
const ROOM_CODE_LENGTH = 7;
const ROOM_EMPTY_GRACE_MS = 20_000;
const ROOM_TTL_MS = 5 * 60_000;
const PUBLIC_BUFFER_LIMIT = 40;
const MESSAGE_CHAR_LIMIT = 1000;
const RATE_LIMIT_PER_MINUTE = 120;
const REQUEST_COOLDOWN_MS = 12_000;
const ACTIVITY_RETAIN_LIMIT = 250;

const handleRoots = [
  "shadow", "night", "ghost", "void", "storm", "raven", "cipher",
  "phantom", "neon", "frost", "blaze", "drift", "echo", "noir",
  "pulse", "vortex", "hex", "spectre", "zero", "crimson", "lunar",
  "static", "onyx", "nova", "ember", "steel", "mirage", "pixel"
];
const handleSuffixes = [
  "owl", "hawk", "fox", "wolf", "lynx", "cat", "byte",
  "x", "zero", "7", "z", "99", "punk", "dev", "runner", "blade"
];
const avatars = ["🜁", "⚡", "✦", "◉", "⬢", "◆", "✶", "☾", "⬡", "◌", "✷", "✧", "⬟", "☄", "◈", "◍"];
const palette = [
  "#00f5ff", "#8b5cf6", "#22c55e", "#f59e0b", "#ef4444",
  "#06b6d4", "#a855f7", "#14b8a6", "#eab308", "#f472b6"
];

function hash32(str) {
  let h = 2166136261;
  for (let i = 0; i < str.length; i++) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

function pick(arr, seed) {
  return arr[seed % arr.length];
}

function randomRoomCode(len = ROOM_CODE_LENGTH) {
  const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
  const bytes = crypto.randomBytes(len);
  let out = "";
  for (let i = 0; i < len; i++) out += alphabet[bytes[i] % alphabet.length];
  return out;
}

function escapeHtml(s) {
  return String(s || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function isImageUrl(text) {
  return /^https?:\/\/\S+\.(png|jpe?g|gif|webp)(\?.*)?$/i.test(text.trim());
}

function buildProfile(userId, usedNames) {
  const seed = hash32(userId);
  function makeName(s) {
    const root = pick(handleRoots, s) || "User";
    const suf = pick(handleSuffixes, s >>> 3) || "Anon";
    return root.charAt(0).toUpperCase() + root.slice(1) + suf.charAt(0).toUpperCase() + suf.slice(1);
  }
  let displayName = makeName(seed);
  let suffix = 0;
  while (usedNames.has(displayName)) {
    suffix += 1;
    displayName = makeName(seed + suffix * 13) + (suffix > 5 ? suffix : '');
  }
  const avatar = avatars[seed % avatars.length];
  const color = palette[seed % palette.length];
  return { displayName, avatar, color };
}

function sanitizeMessage(text) {
  const trimmed = String(text || "").trim();
  if (!trimmed) return { text: "", media: null };

  if (isImageUrl(trimmed)) {
    return { text: "", media: { type: "image", url: trimmed } };
  }

  return { text: escapeHtml(trimmed).slice(0, MESSAGE_CHAR_LIMIT), media: null };
}

/**
 * In-memory state
 */
const clients = new Map(); // ws -> meta
const rooms = new Map(); // roomId -> room
const publicBuffer = [];
const pendingRequests = new Map(); // requestId -> request object
const activity = []; // lightweight event history for debugging/inspection

function pushActivity(event) {
  activity.push({ ts: Date.now(), ...event });
  if (activity.length > ACTIVITY_RETAIN_LIMIT) activity.shift();
}

function rateAllow(meta) {
  const now = Date.now();
  const windowMs = 60_000;
  if (!meta.rate) meta.rate = { count: 0, start: now };
  if (now - meta.rate.start > windowMs) {
    meta.rate.start = now;
    meta.rate.count = 0;
  }
  meta.rate.count += 1;
  return meta.rate.count <= RATE_LIMIT_PER_MINUTE;
}

function getPublicClients() {
  return [...clients.entries()].filter(([, meta]) => meta.currentRoom === "public");
}

function getPublicUsers() {
  return getPublicClients().map(([, meta]) => ({
    userId: meta.userId,
    displayName: meta.displayName,
    avatar: meta.avatar,
    color: meta.color
  }));
}

function broadcastPublicPresence() {
  const users = getPublicUsers();
  const payload = {
    type: "presence_update",
    count: users.length,
    users
  };
  const raw = JSON.stringify(payload);
  for (const [ws, meta] of getPublicClients()) {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.send(raw); } catch {}
    }
  }
}

function sendToSocket(ws, payload) {
  try {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(payload));
  } catch {}
}

function broadcastPublic(payload) {
  const raw = JSON.stringify(payload);
  for (const [ws, meta] of getPublicClients()) {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.send(raw); } catch {}
    }
  }
}

function serializeMessage(msg, viewerId = null) {
  const reactions = {};
  let viewerReactions = [];
  if (msg.reactionUsers) {
    for (const [emoji, set] of Object.entries(msg.reactionUsers)) {
      reactions[emoji] = set.size;
      if (viewerId && set.has(viewerId)) viewerReactions.push(emoji);
    }
  }

  return {
    id: msg.id,
    userId: msg.userId,
    displayName: msg.displayName,
    avatar: msg.avatar,
    color: msg.color,
    text: msg.text,
    ts: msg.ts,
    replyTo: msg.replyTo || null,
    media: msg.media || null,
    reactions,
    viewerReactions
  };
}

function findMessageById(scope, id) {
  if (!id) return null;
  if (scope === "public") {
    return publicBuffer.find(m => m.id === id) || null;
  }
  const room = rooms.get(scope);
  if (!room) return null;
  return room.messages.find(m => m.id === id) || null;
}

function serializeRoomMessages(room, viewerId) {
  return room.messages.slice(-60).map(m => serializeMessage(m, viewerId));
}

function deleteRoom(roomId, reason = "deleted") {
  const room = rooms.get(roomId);
  if (!room) return;

  for (const ws of room.sockets) {
    const meta = clients.get(ws);
    if (meta) {
      meta.currentRoom = "public";
      meta.typing = false;
      sendToSocket(ws, { type: "room_deleted", roomId, reason });
    }
  }

  if (room.emptyTimer) clearTimeout(room.emptyTimer);
  if (room.ttlTimer) clearTimeout(room.ttlTimer);
  rooms.delete(roomId);
  broadcastPublicPresence();
}

function touchRoomTTL(room) {
  room.lastActivityAt = Date.now();
  if (room.ttlTimer) clearTimeout(room.ttlTimer);
  room.ttlTimer = setTimeout(() => {
    if (!rooms.has(room.id)) return;
    const current = rooms.get(room.id);
    if (!current) return;
    if (Date.now() - current.lastActivityAt >= ROOM_TTL_MS) {
      deleteRoom(room.id, "ttl_expired");
    } else {
      touchRoomTTL(current);
    }
  }, ROOM_TTL_MS);
}

function scheduleEmptyRoomDeletion(room) {
  if (room.sockets.size > 0) return;
  if (room.emptyTimer) return;
  room.emptyTimer = setTimeout(() => {
    const current = rooms.get(room.id);
    if (!current) return;
    if (current.sockets.size === 0) deleteRoom(room.id, "empty");
    else current.emptyTimer = null;
  }, ROOM_EMPTY_GRACE_MS);
}

function clearEmptyTimer(room) {
  if (room.emptyTimer) {
    clearTimeout(room.emptyTimer);
    room.emptyTimer = null;
  }
}

function createRoom() {
  let id;
  do { id = randomRoomCode(); } while (rooms.has(id));
  const room = {
    id,
    sockets: new Set(),
    participants: new Set(),
    messages: [],
    createdAt: Date.now(),
    lastActivityAt: Date.now(),
    emptyTimer: null,
    ttlTimer: null
  };
  touchRoomTTL(room);
  rooms.set(id, room);
  return room;
}

function moveSocketOutOfCurrentRoom(ws, meta) {
  if (!meta.currentRoom || meta.currentRoom === "public") return;
  const current = rooms.get(meta.currentRoom);
  if (!current) {
    meta.currentRoom = "public";
    return;
  }
  current.sockets.delete(ws);
  current.participants.delete(meta.userId);
  broadcastRoom(current.id, { type: "user_left_room", roomId: current.id, userId: meta.userId });
  if (current.sockets.size === 0) {
    scheduleEmptyRoomDeletion(current);
  }
  meta.currentRoom = "public";
}

function roomParticipantProfiles(room) {
  const out = [];
  for (const ws of room.sockets) {
    const meta = clients.get(ws);
    if (meta) {
      out.push({
        userId: meta.userId,
        displayName: meta.displayName,
        avatar: meta.avatar,
        color: meta.color
      });
    }
  }
  return out;
}

function broadcastRoom(roomId, payload) {
  const room = rooms.get(roomId);
  if (!room) return;
  const raw = JSON.stringify(payload);
  for (const ws of room.sockets) {
    if (ws.readyState === WebSocket.OPEN) {
      try { ws.send(raw); } catch {}
    }
  }
}

function joinRoom(ws, meta, room, mode = "join") {
  moveSocketOutOfCurrentRoom(ws, meta);
  clearEmptyTimer(room);
  room.sockets.add(ws);
  room.participants.add(meta.userId);
  meta.currentRoom = room.id;
  meta.typing = false;
  touchRoomTTL(room);
  broadcastPublicPresence();
  const joinPayload = {
    type: mode === "create" ? "room_created" : "joined",
    roomId: room.id,
    participants: roomParticipantProfiles(room),
    messages: serializeRoomMessages(room, meta.userId)
  };
  sendToSocket(ws, joinPayload);
  broadcastRoom(room.id, { type: "user_joined_room", roomId: room.id, userId: meta.userId });
}

function getMessageStoreForRoom(roomId) {
  return roomId === "public" ? publicBuffer : rooms.get(roomId)?.messages || null;
}

function findStoredMessage(roomId, messageId) {
  const store = getMessageStoreForRoom(roomId);
  if (!store) return null;
  return store.find(m => m.id === messageId) || null;
}

function updateReactions(store, messageId, userId, emoji) {
  const msg = store.find(m => m.id === messageId);
  if (!msg) return null;
  if (!msg.reactionUsers) msg.reactionUsers = {};

  if (!msg.reactionUsers[emoji]) msg.reactionUsers[emoji] = new Set();
  const set = msg.reactionUsers[emoji];

  if (set.has(userId)) set.delete(userId);
  else set.add(userId);

  if (set.size === 0) delete msg.reactionUsers[emoji];
  return msg;
}

function emitReactionUpdate(scope, roomId, messageId) {
  const msg = scope === "public"
    ? publicBuffer.find(m => m.id === messageId)
    : rooms.get(roomId)?.messages.find(m => m.id === messageId);

  if (!msg) return;

  const payload = {
    type: "reaction_update",
    scope,
    roomId: scope === "room" ? roomId : null,
    message: serializeMessage(msg)
  };

  if (scope === "public") broadcastPublic(payload);
  else broadcastRoom(roomId, payload);
}

function cleanupPendingForUser(userId) {
  for (const [id, req] of pendingRequests.entries()) {
    if (req.fromUserId === userId || req.toUserId === userId) {
      pendingRequests.delete(id);
    }
  }
}

wss.on("connection", (ws) => {
  const userId = crypto.randomUUID();

  // ensure display name uniqueness among active users
  const usedNames = new Set([...clients.values()].map(m => m.displayName));
  const profile = buildProfile(userId, usedNames);

  const meta = {
    userId,
    displayName: profile.displayName,
    avatar: profile.avatar,
    color: profile.color,
    currentRoom: "public",
    typing: false,
    lastRequestAt: 0,
    rate: { count: 0, start: Date.now() }
  };

  clients.set(ws, meta);

  sendToSocket(ws, {
    type: "assign_profile",
    userId: meta.userId,
    displayName: meta.displayName,
    avatar: meta.avatar,
    color: meta.color,
    room: "public"
  });

  sendToSocket(ws, {
    type: "public_buffer",
    messages: publicBuffer.map(m => serializeMessage(m, meta.userId))
  });

  broadcastPublicPresence();

  ws.on("message", (raw) => {
    let obj;
    try {
      obj = JSON.parse(raw.toString());
    } catch {
      sendToSocket(ws, { type: "error", code: "bad_json", message: "Invalid JSON." });
      return;
    }

    if (!rateAllow(meta)) {
      sendToSocket(ws, { type: "error", code: "rate_limited", message: "Rate limit exceeded." });
      return;
    }

    const type = String(obj.type || "");

    if (type === "ping") {
      sendToSocket(ws, { type: "pong" });
      return;
    }

    if (type === "typing") {
      const isTyping = Boolean(obj.isTyping);
      const scope = meta.currentRoom === "public" ? "public" : "room";
      meta.typing = isTyping;
      if (scope === "public") {
        broadcastPublic({
          type: "typing",
          scope: "public",
          userId: meta.userId,
          displayName: meta.displayName,
          isTyping
        });
      } else {
        const room = rooms.get(meta.currentRoom);
        if (room) {
          broadcastRoom(room.id, {
            type: "typing",
            scope: "room",
            roomId: room.id,
            userId: meta.userId,
            displayName: meta.displayName,
            isTyping
          });
        }
      }
      return;
    }

    if (type === "create_room") {
      if (meta.currentRoom !== "public") {
        sendToSocket(ws, { type: "error", code: "already_private", message: "Leave current room first." });
        return;
      }
      const room = createRoom();
      joinRoom(ws, meta, room, "create");
      return;
    }

    if (type === "join_room") {
      const roomId = String(obj.roomId || "").trim();
      if (!roomId) {
        sendToSocket(ws, { type: "error", code: "no_room", message: "No room code provided." });
        return;
      }
      const room = rooms.get(roomId);
      if (!room) {
        sendToSocket(ws, { type: "error", code: "no_room", message: "Room not found or expired." });
        return;
      }
      joinRoom(ws, meta, room, "join");
      return;
    }

    if (type === "leave_room") {
      if (meta.currentRoom !== "public") {
        moveSocketOutOfCurrentRoom(ws, meta);
        meta.currentRoom = "public";
        sendToSocket(ws, { type: "left_room" });
        broadcastPublicPresence();
      }
      return;
    }

    if (type === "private_request") {
      if (meta.currentRoom !== "public") {
        sendToSocket(ws, { type: "error", code: "private_only_public", message: "Private request is only available in public chat." });
        return;
      }

      const targetName = String(obj.targetDisplayName || "").trim();
      if (!targetName) {
        sendToSocket(ws, { type: "error", code: "no_target", message: "Enter a user name." });
        return;
      }

      if (Date.now() - meta.lastRequestAt < REQUEST_COOLDOWN_MS) {
        sendToSocket(ws, { type: "error", code: "cooldown", message: "Please wait before sending another request." });
        return;
      }

      const targetEntry = [...clients.entries()].find(([, m]) =>
        m.currentRoom === "public" && m.displayName.toLowerCase() === targetName.toLowerCase()
      );

      if (!targetEntry) {
        sendToSocket(ws, { type: "error", code: "target_not_found", message: "That user is not available in public chat." });
        return;
      }

      const [targetWs, targetMeta] = targetEntry;
      if (targetMeta.userId === meta.userId) {
        sendToSocket(ws, { type: "error", code: "self_target", message: "You cannot request yourself." });
        return;
      }

      const requestId = crypto.randomUUID();
      const request = {
        requestId,
        fromUserId: meta.userId,
        fromDisplayName: meta.displayName,
        fromAvatar: meta.avatar,
        fromColor: meta.color,
        toUserId: targetMeta.userId,
        toDisplayName: targetMeta.displayName,
        createdAt: Date.now()
      };

      pendingRequests.set(requestId, request);
      meta.lastRequestAt = Date.now();

      sendToSocket(targetWs, {
        type: "private_request_incoming",
        requestId,
        requester: {
          userId: meta.userId,
          displayName: meta.displayName,
          avatar: meta.avatar,
          color: meta.color
        }
      });

      sendToSocket(ws, {
        type: "private_request_sent",
        requestId,
        targetDisplayName: targetMeta.displayName
      });
      return;
    }

    if (type === "private_request_response") {
      const requestId = String(obj.requestId || "");
      const decision = String(obj.decision || "");
      const req = pendingRequests.get(requestId);

      if (!req) {
        sendToSocket(ws, { type: "error", code: "request_missing", message: "Request expired." });
        return;
      }

      if (req.toUserId !== meta.userId) {
        sendToSocket(ws, { type: "error", code: "forbidden", message: "You cannot respond to this request." });
        return;
      }

      const senderEntry = [...clients.entries()].find(([, m]) => m.userId === req.fromUserId);
      const targetEntry = [...clients.entries()].find(([, m]) => m.userId === req.toUserId);

      pendingRequests.delete(requestId);

      if (!senderEntry || !targetEntry) {
        sendToSocket(ws, { type: "error", code: "gone", message: "One user left before response." });
        return;
      }

      const [senderWs, senderMeta] = senderEntry;
      const [targetWs, targetMeta] = targetEntry;

      if (decision === "deny") {
        sendToSocket(senderWs, {
          type: "private_request_denied",
          requestId,
          by: targetMeta.displayName
        });
        sendToSocket(targetWs, {
          type: "private_request_result",
          requestId,
          result: "denied"
        });
        return;
      }

      if (decision !== "accept") {
        sendToSocket(ws, { type: "error", code: "bad_decision", message: "Use accept or deny." });
        return;
      }

      // Only work in public
      if (senderMeta.currentRoom !== "public" || targetMeta.currentRoom !== "public") {
        sendToSocket(ws, { type: "error", code: "not_public", message: "Both users must still be in public chat." });
        return;
      }

      const room = createRoom();
      room.sockets.add(senderWs);
      room.sockets.add(targetWs);
      room.participants.add(senderMeta.userId);
      room.participants.add(targetMeta.userId);
      room.lastActivityAt = Date.now();
      clearEmptyTimer(room);
      touchRoomTTL(room);

      senderMeta.currentRoom = room.id;
      targetMeta.currentRoom = room.id;

      sendToSocket(senderWs, {
        type: "joined",
        roomId: room.id,
        participants: roomParticipantProfiles(room),
        messages: []
      });

      sendToSocket(targetWs, {
        type: "joined",
        roomId: room.id,
        participants: roomParticipantProfiles(room),
        messages: []
      });

      broadcastRoom(room.id, { type: "user_joined_room", roomId: room.id, userId: senderMeta.userId });
      broadcastRoom(room.id, { type: "user_joined_room", roomId: room.id, userId: targetMeta.userId });

      broadcastPublicPresence();
      return;
    }

    if (type === "message") {
      const rawText = String(obj.text || "");
      if (!rawText.trim()) return;

      const replyTo = String(obj.replyTo || "").trim() || null;
      const { text, media } = sanitizeMessage(rawText);
      if (!text && !media) return;

      const msg = {
        id: crypto.randomUUID(),
        userId: meta.userId,
        displayName: meta.displayName,
        avatar: meta.avatar,
        color: meta.color,
        text,
        media,
        ts: Date.now(),
        replyTo,
        reactionUsers: {}
      };

      const scope = meta.currentRoom === "public" ? "public" : "room";
      if (scope === "public") {
        publicBuffer.push(msg);
        if (publicBuffer.length > PUBLIC_BUFFER_LIMIT) publicBuffer.shift();

        broadcastPublic({
          type: "message",
          scope: "public",
          message: serializeMessage(msg, meta.userId)
        });
      } else {
        const room = rooms.get(meta.currentRoom);
        if (!room) {
          meta.currentRoom = "public";
          sendToSocket(ws, { type: "error", code: "room_gone", message: "Room no longer exists." });
          broadcastPublicPresence();
          return;
        }
        room.messages.push(msg);
        touchRoomTTL(room);
        broadcastRoom(room.id, {
          type: "message",
          scope: "room",
          roomId: room.id,
          message: serializeMessage(msg, meta.userId)
        });
      }
      return;
    }

    if (type === "react") {
      const messageId = String(obj.messageId || "");
      const emoji = String(obj.emoji || "").trim();
      if (!messageId || !emoji) return;

      const scope = String(obj.scope || (meta.currentRoom === "public" ? "public" : "room"));
      if (scope === "public") {
        const msg = updateReactions(publicBuffer, messageId, meta.userId, emoji);
        if (!msg) return;
        emitReactionUpdate("public", null, messageId);
      } else {
        const roomId = meta.currentRoom;
        const room = rooms.get(roomId);
        if (!room) return;
        const msg = updateReactions(room.messages, messageId, meta.userId, emoji);
        if (!msg) return;
        touchRoomTTL(room);
        emitReactionUpdate("room", roomId, messageId);
      }
      return;
    }

    sendToSocket(ws, { type: "error", code: "unknown", message: "Unknown message type." });
  });

  ws.on("close", () => {
    const meta = clients.get(ws);
    if (!meta) return;

    if (meta.currentRoom !== "public") {
      const room = rooms.get(meta.currentRoom);
      if (room) {
        room.sockets.delete(ws);
        room.participants.delete(meta.userId);
        broadcastRoom(room.id, { type: "user_left_room", roomId: room.id, userId: meta.userId });
        if (room.sockets.size === 0) scheduleEmptyRoomDeletion(room);
        touchRoomTTL(room);
      }
    }

    cleanupPendingForUser(meta.userId);
    clients.delete(ws);
    broadcastPublicPresence();
  });

  ws.on("error", (err) => {
    console.error("ws error:", err);
  });
});

server.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
