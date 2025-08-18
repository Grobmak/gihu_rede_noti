import 'dotenv/config';
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { Client } from '@notionhq/client';

/* ---------- App ---------- */
const app = express();
app.set('trust proxy', 1);
app.use(helmet());
app.use(cors()); // เปิดกว้างเพื่อไม่พัง ใช้ตามจริงค่อยจำกัด origin
app.use(express.json({ limit: '1mb' }));

/* ---------- Helpers ---------- */
const ok = (res, data) => res.json(data);
const fail = (res, status, code, message, extra = {}) =>
  res.status(status).json({ error: code, message, ...extra });

const bearer = (req) => {
  const h = req.headers.authorization || '';
  const m = h.match(/^Bearer\s+(.+)$/i);
  return m ? m[1] : process.env.NOTION_TOKEN;
};
const getDbId = (req) =>
  req.query.database_id ||
  req.params.database_id ||
  req.body?.database_id ||
  process.env.NOTION_DATABASE_ID;

const notionClient = (req) => {
  const token = bearer(req);
  if (!token) {
    const msg = 'missing NOTION_TOKEN (Bearer token or env)';
    throw Object.assign(new Error(msg), { status: 401, code: 'missing_token' });
  }
  return new Client({ auth: token });
};

/* ---------- DB Meta Cache ---------- */
const metaCache = new Map(); // key = `${dbId}`
const cacheTtlMs = 5 * 60 * 1000;
const now = () => Date.now();

async function getDbMeta(notion, dbId) {
  const key = `${dbId}`;
  const hit = metaCache.get(key);
  if (hit && now() - hit.t < cacheTtlMs) return hit.meta;

  const db = await notion.databases.retrieve({ database_id: dbId });
  const propsByName = {};
  const propsById = {};
  for (const [name, p] of Object.entries(db.properties)) {
    propsByName[name.toLowerCase()] = { id: p.id, type: p.type, name, raw: p };
    propsById[p.id] = { id: p.id, type: p.type, name, raw: p };
  }
  const meta = {
    id: db.id,
    title: (db.title || []).map((r) => r.plain_text).join(''),
    propertiesByName: propsByName,
    propertiesById: propsById,
    raw: db,
  };
  metaCache.set(key, { t: now(), meta });
  return meta;
}

function findProp(meta, key) {
  if (!key) return null;
  const k = String(key);
  const byName = meta.propertiesByName[k.toLowerCase()];
  if (byName) return byName;
  const byId = meta.propertiesById[k];
  if (byId) return byId;
  return null;
}

const asText = (v) => [{ type: 'text', text: { content: String(v ?? '') } }];

function toPropValue(propType, v) {
  switch (propType) {
    case 'title':
      return { title: Array.isArray(v) ? v : asText(v) };
    case 'rich_text':
      return { rich_text: Array.isArray(v) ? v : asText(v) };
    case 'number':
      return { number: v === null || v === '' || v === undefined ? null : Number(v) };
    case 'select':
      return { select: v ? { name: String(v) } : null };
    case 'multi_select': {
      const arr = Array.isArray(v) ? v : String(v || '').split(',').map((s) => s.trim()).filter(Boolean);
      return { multi_select: arr.map((name) => ({ name })) };
    }
    case 'checkbox':
      return { checkbox: !!(v === true || v === 'true' || v === 1 || v === '1') };
    case 'date':
      if (!v) return { date: null };
      if (typeof v === 'string') return { date: { start: v } };
      if (v.start || v.end) return { date: { start: v.start || null, end: v.end || null } };
      return { date: null };
    case 'url':
      return { url: v ? String(v) : null };
    case 'email':
      return { email: v ? String(v) : null };
    case 'phone_number':
      return { phone_number: v ? String(v) : null };
    case 'relation': {
      const ids = Array.isArray(v) ? v : String(v || '').split(',').map((s) => s.trim()).filter(Boolean);
      return { relation: ids.map((id) => ({ id })) };
    }
    case 'people': {
      const ids = Array.isArray(v) ? v : String(v || '').split(',').map((s) => s.trim()).filter(Boolean);
      return { people: ids.map((id) => ({ id })) };
    }
    default:
      // default to rich_text to avoid failure
      return { rich_text: Array.isArray(v) ? v : asText(v) };
  }
}

function buildProperties(meta, obj = {}) {
  const out = {};
  for (const [k, v] of Object.entries(obj)) {
    const p = findProp(meta, k);
    if (!p) continue; // skip unknown to avoid 400
    out[p.name] = toPropValue(p.type, v);
  }
  return out;
}

/* ---------- Filters ---------- */
function buildFilter(meta, prop, op, value) {
  if (!prop) return null;
  const p = findProp(meta, prop);
  if (!p) throw Object.assign(new Error(`unknown property: ${prop}`), { status: 400, code: 'unknown_property' });

  const name = p.name;
  const t = p.type;
  const v = value;

  const numOps = {
    eq: 'equals',
    neq: 'does_not_equal',
    gt: 'greater_than',
    gte: 'greater_than_or_equal_to',
    lt: 'less_than',
    lte: 'less_than_or_equal_to',
  };

  if (t === 'number' && numOps[op]) return { property: name, number: { [numOps[op]]: Number(v) } };
  if (t === 'checkbox') return { property: name, checkbox: { equals: !!(v === true || v === 'true' || v === 1 || v === '1') } };
  if (t === 'select') {
    if (op === 'eq' || op === 'equals') return { property: name, select: { equals: String(v) } };
    if (op === 'neq' || op === 'not') return { property: name, select: { does_not_equal: String(v) } };
    if (op === 'is_empty') return { property: name, select: { is_empty: true } };
    if (op === 'is_not_empty') return { property: name, select: { is_not_empty: true } };
  }
  if (t === 'multi_select') {
    if (op === 'contains') return { property: name, multi_select: { contains: String(v) } };
    if (op === 'not_contains') return { property: name, multi_select: { does_not_contain: String(v) } };
    if (op === 'is_empty') return { property: name, multi_select: { is_empty: true } };
    if (op === 'is_not_empty') return { property: name, multi_select: { is_not_empty: true } };
  }
  if (t === 'date') {
    if (op === 'before') return { property: name, date: { before: String(v) } };
    if (op === 'after') return { property: name, date: { after: String(v) } };
    if (op === 'on_or_before') return { property: name, date: { on_or_before: String(v) } };
    if (op === 'on_or_after') return { property: name, date: { on_or_after: String(v) } };
    if (op === 'is_empty') return { property: name, date: { is_empty: true } };
    if (op === 'is_not_empty') return { property: name, date: { is_not_empty: true } };
    if (op === 'past_week') return { property: name, date: { past_week: {} } };
    if (op === 'next_week') return { property: name, date: { next_week: {} } };
  }
  // text-like
  const textSpec =
    t === 'title' ? 'title' :
    t === 'rich_text' ? 'rich_text' :
    null;
  if (textSpec) {
    if (op === 'contains') return { property: name, [textSpec]: { contains: String(v) } };
    if (op === 'starts_with') return { property: name, [textSpec]: { starts_with: String(v) } };
    if (op === 'eq' || op === 'equals') return { property: name, [textSpec]: { equals: String(v) } };
    if (op === 'neq' || op === 'not') return { property: name, [textSpec]: { does_not_equal: String(v) } };
    if (op === 'is_empty') return { property: name, [textSpec]: { is_empty: true } };
    if (op === 'is_not_empty') return { property: name, [textSpec]: { is_not_empty: true } };
  }

  // fallback: rich_text contains
  return { property: name, rich_text: { contains: String(v) } };
}

function buildSorts(meta, sort, dir) {
  if (!sort) return [];
  const s = String(sort);
  const direction = (String(dir || 'desc').toLowerCase() === 'asc') ? 'ascending' : 'descending';
  if (s === 'last_edited_time') return [{ timestamp: 'last_edited_time', direction }];
  const p = findProp(meta, s);
  if (!p) return [];
  return [{ property: p.name, direction }];
}

/* ---------- Routes ---------- */
app.get('/healthz', (req, res) => ok(res, { ok: true }));

app.get('/db_meta', async (req, res, next) => {
  try {
    const notion = notionClient(req);
    const dbId = getDbId(req);
    if (!dbId) return fail(res, 400, 'missing_database_id', 'database_id is required');
    const meta = await getDbMeta(notion, dbId);
    const props = Object.values(meta.propertiesByName).map((p) => {
      const opt = p.raw?.[p.type]?.options || [];
      return {
        id: p.id,
        name: p.name,
        type: p.type,
        options: Array.isArray(opt) ? opt.map((o) => ({ id: o.id, name: o.name, color: o.color })) : undefined,
      };
    });
    ok(res, { id: meta.id, title: meta.title, properties: props });
  } catch (e) { next(e); }
});

app.get('/db_options', async (req, res, next) => {
  try {
    const notion = notionClient(req);
    const dbId = getDbId(req);
    const propKey = req.query.prop || req.query.property;
    if (!dbId) return fail(res, 400, 'missing_database_id', 'database_id is required');
    if (!propKey) return fail(res, 400, 'missing_property', 'prop is required');
    const meta = await getDbMeta(notion, dbId);
    const p = findProp(meta, propKey);
    if (!p) return fail(res, 400, 'unknown_property', `unknown property: ${propKey}`);
    const opt = p.raw?.[p.type]?.options || [];
    ok(res, { property: p.name, type: p.type, options: opt.map((o) => ({ id: o.id, name: o.name, color: o.color })) });
  } catch (e) { next(e); }
});

app.get('/list_pages', async (req, res, next) => {
  try {
    const notion = notionClient(req);
    const dbId = getDbId(req);
    if (!dbId) return fail(res, 400, 'missing_database_id', 'database_id is required');

    const meta = await getDbMeta(notion, dbId);
    const { prop, op, value, sort, dir, page_size, start_cursor } = req.query;

    const filter = prop ? buildFilter(meta, prop, op || 'contains', value) : undefined;
    const sorts = buildSorts(meta, sort, dir);

    const resp = await notion.databases.query({
      database_id: dbId,
      filter,
      sorts,
      page_size: page_size ? Number(page_size) : 25,
      start_cursor: start_cursor || undefined,
    });

    // slim page output
    const out = resp.results.map((pg) => ({
      id: pg.id,
      url: pg.url,
      last_edited_time: pg.last_edited_time,
      properties: Object.fromEntries(
        Object.entries(pg.properties || {}).map(([k, p]) => {
          switch (p.type) {
            case 'title': return [k, p.title.map((r) => r.plain_text).join('')];
            case 'rich_text': return [k, p.rich_text.map((r) => r.plain_text).join('')];
            case 'number': return [k, p.number];
            case 'select': return [k, p.select?.name || null];
            case 'multi_select': return [k, p.multi_select.map((x) => x.name)];
            case 'checkbox': return [k, p.checkbox];
            case 'date': return [k, p.date];
            case 'url': return [k, p.url];
            case 'email': return [k, p.email];
            case 'phone_number': return [k, p.phone_number];
            case 'relation': return [k, p.relation.map((r) => r.id)];
            default: return [k, null];
          }
        })
      ),
    }));

    ok(res, { results: out, has_more: resp.has_more, next_cursor: resp.next_cursor });
  } catch (e) { next(e); }
});

app.post('/create_page', async (req, res, next) => {
  try {
    const notion = notionClient(req);
    const dbId = getDbId(req);
    if (!dbId) return fail(res, 400, 'missing_database_id', 'database_id is required');

    const meta = await getDbMeta(notion, dbId);
    const { titleProp, title, properties, children } = req.body || {};

    const props = buildProperties(meta, properties || {});
    if (titleProp && title !== undefined) {
      const p = findProp(meta, titleProp);
      if (!p) return fail(res, 400, 'unknown_property', `unknown title property: ${titleProp}`);
      props[p.name] = toPropValue('title', title);
    }

    const page = await notion.pages.create({
      parent: { database_id: dbId },
      properties: props,
      children: Array.isArray(children) ? children : undefined,
    });

    ok(res, { id: page.id, url: page.url });
  } catch (e) { next(e); }
});

/* ---------- Error Handling ---------- */
app.use((err, req, res, _next) => {
  const status = err.status || 500;
  const code = err.code || 'server_error';
  const msg = err.message || 'unexpected';
  fail(res, status, code, msg);
});

/* ---------- Start ---------- */
const port = Number(process.env.PORT || 3000);
app.listen(port, () => {
  // eslint-disable-next-line no-console
  console.log(`notion-mcp-server listening on :${port}`);
});
