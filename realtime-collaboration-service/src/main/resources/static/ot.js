// Client-side OT. Mirrors com.acme.collab.common.OtEngine (components: {t:'r',count} {t:'i',text} {t:'d',count}).
(function (root) {
  function len(c) { return c.t === 'i' ? c.text.length : c.count; }

  function norm(list) {
    const out = [];
    for (const c of list) {
      if (len(c) === 0) continue;
      const l = out[out.length - 1];
      if (l && l.t === c.t) {
        if (c.t === 'i') l.text += c.text; else l.count += c.count;
      } else {
        out.push(c.t === 'i' ? { t: 'i', text: c.text } : { t: c.t, count: c.count });
      }
    }
    return out;
  }

  function peek(list, k, off) {
    if (k >= list.length) return null;
    const c = list[k];
    return c.t === 'i' ? c : { t: c.t, count: c.count - off };
  }

  // a = server op (wins insert ties), b = client op. Returns [a', b'].
  function transform(a, b) {
    const ap = [], bp = [];
    let i = 0, j = 0, oa = 0, ob = 0;
    for (;;) {
      const x = peek(a, i, oa), y = peek(b, j, ob);
      if (!x && !y) break;
      if (x && x.t === 'i') { ap.push(x); bp.push({ t: 'r', count: x.text.length }); i++; oa = 0; continue; }
      if (y && y.t === 'i') { bp.push(y); ap.push({ t: 'r', count: y.text.length }); j++; ob = 0; continue; }
      if (!x || !y) throw new Error('length mismatch');
      const n = Math.min(x.count, y.count);
      if (x.t === 'r' && y.t === 'r') { ap.push({ t: 'r', count: n }); bp.push({ t: 'r', count: n }); }
      else if (x.t === 'd' && y.t === 'r') { ap.push({ t: 'd', count: n }); }
      else if (x.t === 'r' && y.t === 'd') { bp.push({ t: 'd', count: n }); }
      if (oa + n >= len(a[i])) { i++; oa = 0; } else { oa += n; }
      if (ob + n >= len(b[j])) { j++; ob = 0; } else { ob += n; }
    }
    return [norm(ap), norm(bp)];
  }

  function apply(doc, ops) {
    let out = '', pos = 0;
    for (const c of ops) {
      if (c.t === 'r') { out += doc.substr(pos, c.count); pos += c.count; }
      else if (c.t === 'i') { out += c.text; }
      else { pos += c.count; }
    }
    return out + doc.substr(pos);
  }

  // Full-length op that turns oldText into newText (common prefix/suffix diff).
  function diff(oldText, newText) {
    let p = 0;
    const max = Math.min(oldText.length, newText.length);
    while (p < max && oldText[p] === newText[p]) p++;
    let s = 0;
    while (s < max - p && oldText[oldText.length - 1 - s] === newText[newText.length - 1 - s]) s++;
    const del = oldText.length - p - s;
    const ins = newText.substring(p, newText.length - s);
    return norm([{ t: 'r', count: p }, { t: 'd', count: del }, { t: 'i', text: ins }, { t: 'r', count: s }]);
  }

  function transformCursor(cursor, ops) {
    let pos = 0, shift = 0;
    for (const c of ops) {
      if (pos > cursor) break;
      if (c.t === 'r') pos += c.count;
      else if (c.t === 'i') { if (pos <= cursor) shift += c.text.length; }
      else { const ov = Math.min(cursor, pos + c.count) - pos; if (ov > 0) shift -= ov; pos += c.count; }
    }
    return Math.max(0, cursor + shift);
  }

  const api = { transform, apply, diff, norm, transformCursor };
  if (typeof module !== 'undefined' && module.exports) module.exports = api; else root.OT = api;
})(typeof window !== 'undefined' ? window : globalThis);
