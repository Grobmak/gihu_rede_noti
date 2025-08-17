import "dotenv/config";
import express from "express";
import cors from "cors";
import helmet from "helmet";
import { Client } from "@notionhq/client";
import { randomUUID } from "node:crypto";

const app = express();
app.set("trust proxy", true);
app.use(helmet({ crossOriginResourcePolicy: false }));
app.use(cors());
app.use(express.json({ limit: "1mb" }));

// ---- minimal request logging
app.use((req, res, next) => {
  const t0 = Date.now();
  const id = req.headers["x-request-id"] || randomUUID().slice(0, 8);
  req.reqId = String(id).replace(/[^\w-]/g, "").slice(0, 32);
  res.setHeader("x-request-id", req.reqId);
  res.on("finish", () =>
    console.log(
      JSON.stringify({
        id: req.reqId,
        m: req.method,
        u: req.originalUrl,
        s: res.statusCode,
        ms: Date.now() - t0,
      })
    )
  );
  next();
});

// ---------- Auth / Context ----------
function getToken(req) {
  const h = req.headers.authorization || "";
  const m = h.match(/^Bearer\s+(.+)$/i);
  const t = m ? m[1] : process.env.NOTION_TOKEN;
  if (!t) throw Object.assign(new Error("missing NOTION_TOKEN"), { status: 500 });
  if (/\s/.test(t)) throw Object.assign(new Error("token contains whitespace"), { status: 400 });
  return t;
}
function getDbId(req) {
  return (
    req.query.database_id ||
    req.params.database_id ||
    req.body?.database_id ||
    process.env.NOTION_DATABASE_ID
  );
}
function notionClient(req) {
  return new Client({ auth: getToken(req) });
}

// ---------- DB meta cache ----------
const dbMetaCache = new Map();
async function getDbMeta(notion, dbId) {
  if (dbMetaCache.has(dbId)) return dbMetaCache.get(dbId);
  const meta = await notion.databases.retrieve({ database_id: dbId });
  const titlePropName = Object.entries(meta.properties).find(
    ([, v]) => v?.type === "title"
  )?.[0];
  const propTypes = Object.fromEntries(
    Object.entries(meta.properties).map(([k, v]) => [k, v.type])
  );
  const out = { titlePropName, propTypes };
  dbMetaCache.set(dbId, out);
  return out;
}

// ---------- Helpers ----------
function parseBool(v) { if (typeof v === "boolean") return v; return String(v).toLowerCase() === "true"; }
function extractTitle(p) {
  const props = p.properties || {};
  for (const [, v] of Object.entries(props)) {
    if (v?.type === "title" && Array.isArray(v.title) && v.title.length) {
      return v.title[0].plain_text || "(untitled)";
    }
  }
  return "(untitled)";
}
function normalizeKey(s) {
  if (s == null) return "";
  let r = String(s);
  try { r = decodeURIComponent(r); } catch {}
  return r.toLocaleLowerCase().normalize("NFKC").replace(/[\p{Z}\p{P}]/gu, "");
}
const PAGE_SIZE_MAX = 100;
const SORTS_MAX = 5;
const FILTER_TERMS_MAX = 50;

function fail(res, status, code, message, extra = {}) {
  return res.status(status).json({ error: code, message, ...extra });
}
const aw = (fn) => (req, res) =>
  Promise.resolve(fn(req, res)).catch((e) => {
    const status = e.status || 500;
    const code = e.code || (status === 400 ? "bad_request" : "server_error");
    fail(res, status, code, e.message);
  });

function clampPageSize(v) {
  return Math.min(Math.max(parseInt(v || "50", 10) || 50, 1), PAGE_SIZE_MAX);
}
function parseFields(req) {
  const f = (req.query.fields || req.body?.fields || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const flat = String(req.query.flat || req.body?.flat || "0") === "1";
  return { fields: f, flat };
}
function countFilterNodes(node) {
  let c = 0;
  (function w(n) {
    if (!n || typeof n !== "object") return;
    if (Array.isArray(n)) return n.forEach(w);
    if (n.property || n.or || n.and) c++;
    if (n.and) w(n.and);
    if (n.or) w(n.or);
    Object.values(n).forEach(w);
  })(node);
  return c;
}

// ---- Retry Notion 429/5xx
async function queryWithRetry(notion, query, tries = 3) {
  let delay = 200;
  for (let i = 0; i < tries; i++) {
    try {
      return await notion.databases.query(query);
    } catch (e) {
      const retriable = [429, 502, 503, 504].includes(e.status);
      if (!retriable || i === tries - 1) throw e;
      const ra = Number(e.headers?.["retry-after"] || 0) * 1000;
      const wait = ra || delay + Math.floor(Math.random() * 100);
      await new Promise((r) => setTimeout(r, wait));
      delay *= 2;
    }
  }
}

// ---------- Aliases (TH/EN) ----------
const PROP_ALIASES = {
  "ราคา": "Budget",
  "งบประมาณ": "Budget",
  "เปอร์เซ็นต์ความคืบหน้า": "Completion %",
  "percent": "Completion %",
  "progress": "Completion %",
  "สถานะ": "Status",
  "แท็ก": "Tags",
  "แแท็ก": "Tags",
};
const ALIAS_OUT = {};
for (const [k, v] of Object.entries(PROP_ALIASES)) {
  if (/[^\u0000-\u007F]/.test(k) && !ALIAS_OUT[v]) ALIAS_OUT[v] = k;
}
function resolveProp(meta, raw) {
  const want = normalizeKey(raw);
  if (!want) return null;
  if (PROP_ALIASES[raw] && meta.propTypes[PROP_ALIASES[raw]]) return PROP_ALIASES[raw];
  const aliasKey = Object.keys(PROP_ALIASES).find((k) => normalizeKey(k) === want);
  if (aliasKey && meta.propTypes[PROP_ALIASES[aliasKey]]) return PROP_ALIASES[aliasKey];
  if (meta.propTypes[raw]) return raw;
  const keys = Object.keys(meta.propTypes);
  let found = keys.find((k) => normalizeKey(k) === want);
  if (!found) found = keys.find((k) => normalizeKey(k).includes(want));
  return found || null;
}

// ---------- Filters / Sorts ----------
function remapPropName(meta, raw) {
  const real = resolveProp(meta, raw);
  if (!real) throw Object.assign(new Error(`unknown property: ${raw}`), { status: 400 });
  return real;
}
function mapFilterProps(node, meta) {
  if (Array.isArray(node)) return node.map((n) => mapFilterProps(n, meta));
  if (node && typeof node === "object") {
    const out = {};
    for (const [k, v] of Object.entries(node)) {
      if (k === "property") out.property = remapPropName(meta, v);
      else out[k] = mapFilterProps(v, meta);
    }
    return out;
  }
  return node;
}

// shorthand builders for GET ?propN/opN/valueN and groups gX_propN...
function buildFilterFromQSShorthand(q, meta) {
  const andTerms = [];
  const orGroups = [];
  const numOps = { eq: "equals", gt: "greater_than", gte: "greater_than_or_equal_to", lt: "less_than", lte: "less_than_or_equal_to" };
  const textOps = { contains: "contains", equals: "equals", starts_with: "starts_with", ends_with: "ends_with", is_empty: "is_empty", not_empty: "is_not_empty" };

  const parseTerm = (prop, op, val, values) => {
    const real = resolveProp(meta, prop);
    if (!real) throw Object.assign(new Error(`unknown property: ${prop}`), { status: 400 });
    const type = meta.propTypes[real];
    op = String(op || "").toLowerCase();

    if (type === "date") {
      const ok = new Set(["on_or_after","on_or_before","equals","before","after","past_week","past_month","past_year","next_week","next_month","next_year","is_empty","is_not_empty"]);
      if (!ok.has(op)) throw Object.assign(new Error(`unsupported date op: ${op}`), { status: 400 });
      if (op.includes("empty")) return { property: real, date: { [op]: true } };
      if (op.startsWith("past_") || op.startsWith("next_")) return { property: real, date: { [op]: {} } };
      return { property: real, date: { [op]: String(val) } };
    }
    if (type === "checkbox") return { property: real, checkbox: { equals: val === true || String(val).toLowerCase() === "true" } };
    if (type === "number") {
      const key = numOps[op];
      if (!key) throw Object.assign(new Error(`unsupported number op: ${op}`), { status: 400 });
      return { property: real, number: { [key]: Number(val) } };
    }
    if (type === "select" || type === "status") {
      const key = op === "eq" ? "equals" : op === "neq" ? "does_not_equal" : null;
      if (!key) throw Object.assign(new Error(`unsupported ${type} op: ${op}`), { status: 400 });
      return { property: real, [type]: { [key]: String(val) } };
    }
    if (type === "multi_select") {
      if (op === "contains") return { property: real, multi_select: { contains: String(val) } };
      if (op === "not_contains") return { property: real, multi_select: { does_not_contain: String(val) } };
      if (op === "contains_all") {
        const arr = String(values || "").split("|").filter(Boolean);
        if (!arr.length) throw Object.assign(new Error("valuesN required: a|b|c"), { status: 400 });
        return arr.map((v) => ({ property: real, multi_select: { contains: String(v) } }));
      }
      throw Object.assign(new Error(`unsupported multi_select op: ${op}`), { status: 400 });
    }
    if (["title", "rich_text", "url", "email", "phone_number"].includes(type)) {
      const key = textOps[op];
      if (!key) throw Object.assign(new Error(`unsupported text op: ${op}`), { status: 400 });
      return { property: real, [type]: key.includes("empty") ? { [key]: true } : { [key]: String(val) } };
    }
    throw Object.assign(new Error(`unsupported property type: ${type}`), { status: 400 });
  };

  for (let i = 1; i <= 10; i++) {
    const p = q[`prop${i}`];
    if (!p) continue;
    const t = parseTerm(p, q[`op${i}`], q[`value${i}`], q[`values${i}`]);
    Array.isArray(t) ? andTerms.push(...t) : andTerms.push(t);
  }
  for (let g = 1; g <= 5; g++) {
    const terms = [];
    for (let i = 1; i <= 10; i++) {
      const p = q[`g${g}_prop${i}`];
      if (!p) continue;
      const t = parseTerm(p, q[`g${g}_op${i}`], q[`g${g}_value${i}`], q[`g${g}_values${i}`]);
      Array.isArray(t) ? terms.push(...t) : terms.push(t);
    }
    if (terms.length) orGroups.push({ or: terms });
  }

  const nodes = [...andTerms, ...orGroups];
  if (!nodes.length) return undefined;
  return nodes.length === 1 ? nodes[0] : { and: nodes };
}

function buildSortsFromQSShorthand(q, meta) {
  const out = [];
  for (let i = 1; i <= 5; i++) {
    const by = q[`sort_by${i}`];
    if (!by) continue;
    const dir = (q[`sort_dir${i}`] || "ascending").toLowerCase();
    const s = normalizeKey(by);
    if (s === normalizeKey("Last edited time")) { out.push({ timestamp: "last_edited_time", direction: dir }); continue; }
    if (s === normalizeKey("Created time")) { out.push({ timestamp: "created_time", direction: dir }); continue; }
    const real = resolveProp(meta, by);
    if (!real) throw Object.assign(new Error(`unknown sort property: ${by}`), { status: 400 });
    out.push({ property: real, direction: dir });
  }
  return out.length ? out : undefined;
}

function buildFilterFromQuery(q, meta) {
  if (q.filters != null) {
    if (typeof q.filters === "object") {
      const mapped = mapFilterProps(q.filters, meta);
      if (countFilterNodes(mapped) > FILTER_TERMS_MAX)
        throw Object.assign(new Error("filter too complex"), { status: 400, code: "too_complex" });
      return mapped;
    }
    if (typeof q.filters === "string") {
      try {
        const parsed = JSON.parse(q.filters);
        const mapped = mapFilterProps(parsed, meta);
        if (countFilterNodes(mapped) > FILTER_TERMS_MAX)
          throw Object.assign(new Error("filter too complex"), { status: 400, code: "too_complex" });
        return mapped;
      } catch {
        if (/[{}]/.test(q.filters))
          throw Object.assign(new Error("filters must be URL-encoded JSON"), { status: 400, code: "invalid_json" });
        throw Object.assign(new Error("filters must be valid JSON"), { status: 400, code: "invalid_json" });
      }
    }
    throw Object.assign(new Error("filters must be valid JSON"), { status: 400, code: "invalid_json" });
  }

  const and = [];
  if (q.q) {
    const p = meta.titlePropName || "Name";
    and.push({ property: p, title: { contains: String(q.q) } });
  }
  if (q.prop && q.op) {
    const propName = resolveProp(meta, q.prop);
    if (!propName) throw Object.assign(new Error(`unknown property: ${q.prop}`), { status: 400 });
    const op = String(q.op).toLowerCase();
    const type = meta.propTypes[propName];
    const v = q.value ?? "";
    const values = (q.values || "").split("|").filter(Boolean);

    if (type === "date" && (q.date_from || q.date_to)) {
      const d = {};
      if (q.date_from) d.on_or_after = q.date_from;
      if (q.date_to) d.on_or_before = q.date_to;
      and.push({ property: propName, date: d });
    } else if (type === "checkbox") {
      and.push({ property: propName, checkbox: { equals: parseBool(v) } });
    } else if (type === "number") {
      const numOps = { eq:"equals", gt:"greater_than", gte:"greater_than_or_equal_to", lt:"less_than", lte:"less_than_or_equal_to" };
      const key = numOps[op];
      if (!key) throw Object.assign(new Error(`unsupported number op: ${op}`), { status: 400 });
      and.push({ property: propName, number: { [key]: Number(v) } });
    } else if (type === "select" || type === "status") {
      const key = op === "eq" ? "equals" : op === "neq" ? "does_not_equal" : null;
      if (!key) throw Object.assign(new Error(`unsupported ${type} op: ${op}`), { status: 400 });
      and.push({ property: propName, [type]: { [key]: String(v) } });
    } else if (type === "multi_select") {
      if (op === "contains")
        and.push({ property: propName, multi_select: { contains: String(v) } });
      else if (op === "not_contains")
        and.push({ property: propName, multi_select: { does_not_contain: String(v) } });
      else if (op === "contains_all") {
        if (!values.length) throw Object.assign(new Error("values required: a|b|c"), { status: 400 });
        values.forEach(val => and.push({ property: propName, multi_select: { contains: String(val) } }));
      } else {
        throw Object.assign(new Error(`unsupported multi_select op: ${op}`), { status: 400 });
      }
    } else if (["title","rich_text","url","email","phone_number"].includes(type)) {
      const map = { contains:"contains", equals:"equals", starts_with:"starts_with", ends_with:"ends_with", is_empty:"is_empty", not_empty:"is_not_empty" };
      const key = map[op];
      if (!key) throw Object.assign(new Error(`unsupported text op: ${op}`), { status: 400 });
      if (key === "is_empty" || key === "is_not_empty")
        and.push({ property: propName, [type]: { [key]: true } });
      else and.push({ property: propName, [type]: { [key]: String(v) } });
    } else if (type === "date") {
      const dateOps = new Set([
        "on_or_after","on_or_before","equals","before","after",
        "past_week","past_month","past_year","next_week","next_month","next_year",
        "is_empty","is_not_empty"
      ]);
      if (!dateOps.has(op)) throw Object.assign(new Error(`unsupported date op: ${op}`), { status: 400 });
      if (op === "is_empty" || op === "is_not_empty")
        and.push({ property: propName, date: { [op]: true } });
      else if (op.startsWith("past_") || op.startsWith("next_"))
        and.push({ property: propName, date: { [op]: {} } });
      else
        and.push({ property: propName, date: { [op]: String(v) } });
    } else {
      throw Object.assign(new Error(`unsupported property type: ${type}`), { status: 400 });
    }
  }
  if (!and.length) return undefined;
  const out = and.length === 1 ? and[0] : { and };
  if (countFilterNodes(out) > FILTER_TERMS_MAX)
    throw Object.assign(new Error("filter too complex"), { status: 400, code: "too_complex" });
  return out;
}
function buildSortsFromQuery(q, meta) {
  if (q.sorts != null) {
    if (Array.isArray(q.sorts)) {
      const m = q.sorts.map(s => s.property ? ({ ...s, property: remapPropName(meta, s.property) }) : s);
      return m.slice(0, SORTS_MAX);
    }
    if (typeof q.sorts === "string") {
      try {
        const arr = JSON.parse(q.sorts);
        if (!Array.isArray(arr)) throw new Error();
        const m = arr.map(s => s.property ? ({ ...s, property: remapPropName(meta, s.property) }) : s);
        return m.slice(0, SORTS_MAX);
      } catch {
        throw Object.assign(new Error("sorts must be a JSON array"), { status: 400, code: "invalid_json" });
      }
    }
    throw Object.assign(new Error("sorts must be a JSON array"), { status: 400, code: "invalid_json" });
  }
  const short = buildSortsFromQSShorthand(q, meta);
  if (short) return short.slice(0, SORTS_MAX);
  if (q.sort_by) {
    const s = normalizeKey(q.sort_by);
    const dir = (q.sort_dir || "ascending").toLowerCase();
    if (s === normalizeKey("Last edited time")) return [{ timestamp: "last_edited_time", direction: dir }];
    if (s === normalizeKey("Created time"))     return [{ timestamp: "created_time",     direction: dir }];
    const prop = resolveProp(meta, q.sort_by);
    if (!prop) throw Object.assign(new Error(`unknown sort property: ${q.sort_by}`), { status: 400 });
    return [{ property: prop, direction: dir }];
  }
  return undefined;
}

// ---------- Coercers & Normalizer ----------
function asPlainText(val) {
  const s = Array.isArray(val) ? val.join(", ") : String(val ?? "");
  return [{ type: "text", text: { content: s } }];
}
function coerceByType(type, val) {
  switch (type) {
    case "title": return { title: asPlainText(val) };
    case "rich_text": return { rich_text: asPlainText(val) };
    case "number": return { number: (val === "" || val == null) ? null : Number(val) };
    case "select":
    case "status":
      if (val == null || val === "") return { [type]: null };
      return { [type]: { name: String(val) } };
    case "multi_select":
      if (!val) return { multi_select: [] };
      if (Array.isArray(val)) return { multi_select: val.map(n => ({ name: String(n) })) };
      return { multi_select: String(val).split(",").map(s => ({ name: s.trim() })) };
    case "checkbox": return { checkbox: Boolean(val) };
    case "date":
      if (!val) return { date: null };
      if (typeof val === "string") return { date: { start: val } };
      if (val.start || val.date) return { date: { start: val.start || val.date, end: val.end ?? null } };
      return { date: null };
    case "url": return { url: val ? String(val) : null };
    case "email": return { email: val ? String(val) : null };
    case "phone_number": return { phone_number: val ? String(val) : null };
    case "relation":
      if (!val) return { relation: [] };
      const arr = Array.isArray(val) ? val : String(val).split(",").map(s=>s.trim());
      return { relation: arr.map(id => ({ id })) };
    default: return { rich_text: asPlainText(val) };
  }
}
function slimProp([k, p]) {
  const t = p.type;
  switch (t) {
    case "title": return [k, p.title.map(r => r.plain_text).join("")];
    case "rich_text": return [k, p.rich_text.map(r => r.plain_text).join("")];
    case "number": return [k, p.number];
    case "select": return [k, p.select?.name ?? null];
    case "status": return [k, p.status?.name ?? null];
    case "multi_select": return [k, p.multi_select.map(o => o.name)];
    case "checkbox": return [k, p.checkbox];
    case "date": return [k, p.date?.start ?? null];
    case "url": return [k, p.url ?? null];
    case "email": return [k, p.email ?? null];
    case "phone_number": return [k, p.phone_number ?? null];
    case "relation": return [k, p.relation?.map(r => r.id) ?? []];
    default: return [k, null];
  }
}
function normalizePage(page) {
  const propsSlim = Object.fromEntries(Object.entries(page.properties || {}).map(slimProp));
  const propsAlias = {};
  for (const [k, v] of Object.entries(propsSlim)) {
    const th = ALIAS_OUT[k];
    if (th && !(th in propsAlias)) propsAlias[th] = v;
  }
  return {
    id: page.id,
    title: extractTitle(page),
    created_time: page.created_time,
    last_edited_time: page.last_edited_time,
    props: propsSlim,
    props_alias: propsAlias
  };
}
function projectFields(page, fields, meta) {
  if (!fields?.length) return null;
  const want = new Set(fields.map((f) => resolveProp(meta, f) || f));
  const kv = Object.entries(page.properties || {})
    .filter(([k]) => want.has(k))
    .map(slimProp);
  return Object.fromEntries(kv);
}

// ---------- Routes: health & meta ----------
app.get("/health", aw(async (req, res) => {
  const hasEnvToken = !!process.env.NOTION_TOKEN;
  const hasEnvDb = !!process.env.NOTION_DATABASE_ID;
  try {
    const notion = notionClient(req);
    const me = await notion.users.me();
    res.json({ ok: true, auth: "ok", me: { type: me.type }, env: { hasToken: hasEnvToken, hasDB: hasEnvDb } });
  } catch (e) {
    res.status(200).json({ ok: false, auth: "fail", error: { status: e.status || 500, code: e.code || "error", message: e.message }, env: { hasToken: hasEnvToken, hasDB: hasEnvDb } });
  }
}));
app.get("/whoami", aw(async (req, res) => {
  const notion = notionClient(req);
  const me = await notion.users.me();
  res.json(me);
}));
app.get("/db_meta", aw(async (req, res) => {
  const dbId = getDbId(req);
  if (!dbId) return fail(res, 400, "missing_database_id", "missing database_id");
  const notion = notionClient(req);
  const meta = await getDbMeta(notion, dbId);
  res.json(meta);
}));
app.get("/db_props", aw(async (req, res) => {
  const dbId = getDbId(req);
  if (!dbId) return fail(res, 400, "missing_database_id", "missing database_id");
  const notion = notionClient(req);
  const meta = await getDbMeta(notion, dbId);
  res.json(meta.propTypes);
}));

// ---------- list_pages (GET/POST) ----------
async function _handleListPages(req, res) {
  const dbId = getDbId(req);
  if (!dbId) return fail(res, 400, "missing_database_id", "missing database_id (ENV or query/path/body)");
  const notion = notionClient(req);
  const meta = await getDbMeta(notion, dbId);

  const src = req.method === "POST" ? req.body : req.query;
  const page_size = clampPageSize(src.page_size);
  const start_cursor = src.start_cursor;
  const filter = buildFilterFromQuery(src, meta);
  const sorts = buildSortsFromQuery(src, meta);
  const { fields, flat } = parseFields(req);
  const includeProps = String(src.include_props || "").toLowerCase() === "true";

  const query = { database_id: dbId, page_size };
  if (start_cursor) query.start_cursor = start_cursor;
  if (filter) query.filter = filter;
  if (sorts) query.sorts = sorts;

  const out = await queryWithRetry(notion, query);
  const rows = out.results.map((p) => {
    const base = {
      id: p.id,
      title: extractTitle(p),
      created_time: p.created_time,
      last_edited_time: p.last_edited_time,
    };
    if (fields.length) {
      const proj = projectFields(p, fields, meta);
      return proj ? (flat ? { ...base, ...proj } : { ...base, props: proj }) : base;
    }
    if (includeProps) return { ...base, properties: p.properties };
    return base;
  });

  res.json({
    pages: rows,
    has_more: out.has_more,
    next_cursor: out.next_cursor || null,
    meta: { titleProp: meta.titlePropName },
  });
}
const handleListPages = aw(_handleListPages);
app.get("/list_pages", handleListPages);
app.get("/list_pages/:database_id", handleListPages);
app.post("/list_pages", handleListPages);

// ---------- get/create/update/archive/upsert ----------
app.get("/get_page/:id", aw(async (req, res) => {
  const notion = notionClient(req);
  const page = await notion.pages.retrieve({ page_id: req.params.id });
  const normalize = String(req.query.normalize || "1") !== "0";
  res.json(normalize ? normalizePage(page) : page);
}));

app.post("/create_page", aw(async (req, res) => {
  let dbId = getDbId(req);
  if (!dbId) return fail(res, 400, "missing_database_id", "missing database_id");
  const notion = notionClient(req);
  const meta = await getDbMeta(notion, dbId);
  const inputProps = req.body?.props || {};
  const props = {};
  for (const [k, v] of Object.entries(inputProps)) {
    const real = resolveProp(meta, k);
    if (!real) return fail(res, 400, "unknown_property", k);
    const t = meta.propTypes[real];
    props[real] = coerceByType(t, v);
  }
  const titleKey = meta.titlePropName;
  if (!props[titleKey]) {
    const fallback = req.body?.title || "Untitled " + new Date().toISOString();
    props[titleKey] = { title: asPlainText(fallback) };
  }
  const created = await notion.pages.create({
    parent: { database_id: dbId },
    properties: props,
  });
  res.json({ ok: true, id: created.id, page: normalizePage(created) });
}));

app.patch("/update_props/:id", aw(async (req, res) => {
  const notion = notionClient(req);
  let dbId = getDbId(req);
  let page = null;
  if (!dbId) {
    page = await notion.pages.retrieve({ page_id: req.params.id });
    if (page?.parent?.type === "database_id") dbId = page.parent.database_id;
  }
  if (!dbId) return fail(res, 400, "missing_database_id", "missing database_id");
  const meta = await getDbMeta(notion, dbId);
  if (!page) page = await notion.pages.retrieve({ page_id: req.params.id });

  const inputProps = req.body?.props || {};
  const props = {};
  for (const [k, v] of Object.entries(inputProps)) {
    const real = resolveProp(meta, k);
    if (!real) continue;
    const t = page.properties[real]?.type || meta.propTypes[real];
    props[real] = coerceByType(t, v);
  }
  const updated = await notion.pages.update({ page_id: req.params.id, properties: props });
  res.json({ ok: true, id: updated.id, page: normalizePage(updated) });
}));

app.post("/archive_page/:id", aw(async (req, res) => {
  const notion = notionClient(req);
  const updated = await notion.pages.update({ page_id: req.params.id, archived: true });
  res.json({ ok: true, id: updated.id });
}));

app.post("/upsert_page", aw(async (req, res) => {
  const dbId = getDbId(req);
  if (!dbId) return fail(res, 400, "missing_database_id", "missing database_id");
  const { key_prop, key_value, props: inputProps = {}, create_title } = req.body || {};
  if (!key_prop || key_value == null) return fail(res, 400, "missing_key", "key_prop and key_value required");

  const notion = notionClient(req);
  const meta = await getDbMeta(notion, dbId);
  const realKey = resolveProp(meta, key_prop);
  if (!realKey) return fail(res, 400, "unknown_property", key_prop);

  // build equals filter for key
  const t = meta.propTypes[realKey];
  let keyFilter;
  if (["title", "rich_text", "url", "email", "phone_number"].includes(t)) keyFilter = { property: realKey, [t]: { equals: String(key_value) } };
  else if (t === "number") keyFilter = { property: realKey, number: { equals: Number(key_value) } };
  else if (t === "select" || t === "status") keyFilter = { property: realKey, [t]: { equals: String(key_value) } };
  else if (t === "multi_select") keyFilter = { property: realKey, multi_select: { contains: String(key_value) } };
  else return fail(res, 400, "unsupported_key_type", `type=${t}`);

  const found = await queryWithRetry(notion, { database_id: dbId, page_size: 1, filter: keyFilter });
  const props = {};
  for (const [k, v] of Object.entries(inputProps)) {
    const real = resolveProp(meta, k);
    if (!real) return fail(res, 400, "unknown_property", k);
    const tt = meta.propTypes[real];
    props[real] = coerceByType(tt, v);
  }

  if (found.results.length) {
    const pg = found.results[0];
    const updated = await notion.pages.update({ page_id: pg.id, properties: props });
    return res.json({ ok: true, action: "update", id: updated.id, page: normalizePage(updated) });
  } else {
    const titleKey = meta.titlePropName;
    if (!props[titleKey]) {
      props[titleKey] = { title: asPlainText(create_title || String(key_value)) };
    }
    const created = await notion.pages.create({ parent: { database_id: dbId }, properties: props });
    return res.json({ ok: true, action: "create", id: created.id, page: normalizePage(created) });
  }
}));

// ---------- simple text search across title+rich_text in DB ----------
app.get("/search_pages", aw(async (req, res) => {
  const dbId = getDbId(req);
  if (!dbId) return fail(res, 400, "missing_database_id", "missing database_id");
  const notion = notionClient(req);
  const meta = await getDbMeta(notion, dbId);
  const q = String(req.query.q || "").trim();
  if (!q) return fail(res, 400, "missing_q", "q required");
  const terms = [];
  // title
  if (meta.titlePropName) terms.push({ property: meta.titlePropName, title: { contains: q } });
  // rich_text props
  Object.entries(meta.propTypes)
    .filter(([, t]) => t === "rich_text")
    .slice(0, 20)
    .forEach(([k]) => terms.push({ property: k, rich_text: { contains: q } }));
  const filter = terms.length === 1 ? terms[0] : { or: terms };
  const out = await queryWithRetry(notion, { database_id: dbId, page_size: clampPageSize(req.query.page_size), filter });
  res.json({
    pages: out.results.map(normalizePage),
    has_more: out.has_more,
    next_cursor: out.next_cursor || null,
  });
}));

app.get("/db_schema", aw(async (req, res) => {
  const dbId = getDbId(req);
  if (!dbId) return fail(res, 400, "missing_database_id", "missing database_id");
  const notion = notionClient(req);
  const meta = await notion.databases.retrieve({ database_id: dbId });
  const props = Object.fromEntries(Object.entries(meta.properties).map(([k, v]) => {
    const base = { type: v.type };
    if (v.type === "status") base.options = v.status.options.map(o => ({ name: o.name, color: o.color }));
    if (v.type === "select") base.options = v.select.options.map(o => ({ name: o.name, color: o.color }));
    return [k, base];
  }));
  res.json({ id: meta.id, props });
}));


// ---------- global fallback ----------
app.use((req, res) => res.status(404).json({ error: "not_found" }));

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`Notion MCP server :${port}`));
