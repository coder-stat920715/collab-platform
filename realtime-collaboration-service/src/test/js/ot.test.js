// Run: node src/test/js/ot.test.js
const OT = require('../../main/resources/static/ot.js');
let seed = 42;
const rnd = (n) => { seed = (seed * 1103515245 + 12345) & 0x7fffffff; return seed % n; };
const text = (n) => Array.from({ length: n }, () => String.fromCharCode(97 + rnd(26))).join('');
function randomOp(len) {
  const ops = []; let rem = len;
  while (rem > 0) {
    const k = 1 + rnd(rem), c = rnd(3);
    if (c === 0) { ops.push({ t: 'r', count: k }); rem -= k; }
    else if (c === 1) { ops.push({ t: 'd', count: k }); rem -= k; }
    else ops.push({ t: 'i', text: text(1 + rnd(3)) });
  }
  if (rnd(2)) ops.push({ t: 'i', text: text(1 + rnd(3)) });
  return OT.norm(ops);
}
for (let n = 0; n < 50000; n++) {
  const doc = text(rnd(30));
  const a = randomOp(doc.length), b = randomOp(doc.length);
  const [ap, bp] = OT.transform(a, b);
  const x = OT.apply(OT.apply(doc, a), bp), y = OT.apply(OT.apply(doc, b), ap);
  if (x !== y) throw new Error('diverged ' + JSON.stringify({ doc, a, b, x, y }));
}
for (let n = 0; n < 20000; n++) {
  const o = text(rnd(25)), w = text(rnd(25));
  if (OT.apply(o, OT.diff(o, w)) !== w) throw new Error('diff broken ' + o + ' -> ' + w);
}
console.log('JS OT: TP1 and diff OK');
