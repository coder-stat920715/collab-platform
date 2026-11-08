package com.acme.collab.common;

import com.acme.collab.common.Component.Delete;
import com.acme.collab.common.Component.Insert;
import com.acme.collab.common.Component.Retain;
import java.util.ArrayList;
import java.util.List;

/**
 * Operational Transformation for plain text (retain / insert / delete).
 * Operations are canonical: retain + delete lengths equal the base document length.
 * Guarantees TP1: apply(apply(d,a), b') == apply(apply(d,b), a').
 */
public final class OtEngine {
  private OtEngine() {}

  public record Pair(List<Component> a, List<Component> b) {}

  /**
   * @param a operation already sequenced on the server (wins insert ties)
   * @param b incoming concurrent operation
   * @return (a', b') where a' applies after b, and b' applies after a
   */
  public static Pair transform(List<Component> a, List<Component> b) {
    List<Component> ap = new ArrayList<>();
    List<Component> bp = new ArrayList<>();
    Cursor ca = new Cursor(a);
    Cursor cb = new Cursor(b);
    while (ca.peek() != null || cb.peek() != null) {
      if (ca.peek() instanceof Insert ia) {
        ap.add(ia);
        bp.add(new Retain(ia.text().length()));
        ca.skipInsert();
        continue;
      }
      if (cb.peek() instanceof Insert ib) {
        bp.add(ib);
        ap.add(new Retain(ib.text().length()));
        cb.skipInsert();
        continue;
      }
      Component x = ca.peek();
      Component y = cb.peek();
      if (x == null || y == null) {
        throw new IllegalArgumentException("Operations have different base lengths");
      }
      int n = Math.min(Cursor.len(x), Cursor.len(y));
      if (x instanceof Retain && y instanceof Retain) {
        ap.add(new Retain(n));
        bp.add(new Retain(n));
      } else if (x instanceof Delete && y instanceof Retain) {
        ap.add(new Delete(n));
      } else if (x instanceof Retain && y instanceof Delete) {
        bp.add(new Delete(n));
      } // delete vs delete: same text already removed, emit nothing
      ca.advance(n);
      cb.advance(n);
    }
    return new Pair(normalize(ap), normalize(bp));
  }

  public static String apply(String doc, List<Component> ops) {
    StringBuilder out = new StringBuilder();
    int pos = 0;
    for (Component c : ops) {
      switch (c) {
        case Retain r -> {
          if (pos + r.count() > doc.length()) throw new IllegalArgumentException("retain beyond end");
          out.append(doc, pos, pos + r.count());
          pos += r.count();
        }
        case Insert i -> out.append(i.text());
        case Delete d -> {
          if (pos + d.count() > doc.length()) throw new IllegalArgumentException("delete beyond end");
          pos += d.count();
        }
      }
    }
    out.append(doc, pos, doc.length());
    return out.toString();
  }

  /** Length of the document an operation can be applied to. */
  public static int baseLength(List<Component> ops) {
    int n = 0;
    for (Component c : ops) {
      if (c instanceof Retain r) n += r.count();
      else if (c instanceof Delete d) n += d.count();
    }
    return n;
  }

  /** Length of the document after the operation is applied. */
  public static int targetLength(List<Component> ops) {
    int n = 0;
    for (Component c : ops) {
      if (c instanceof Retain r) n += r.count();
      else if (c instanceof Insert i) n += i.text().length();
    }
    return n;
  }

  /** Transforms a cursor offset across an operation that was applied to the document. */
  public static int transformCursor(int cursor, List<Component> ops) {
    int pos = 0;     // position in the old document
    int result = cursor;
    int shift = 0;
    for (Component c : ops) {
      if (pos > cursor) break;
      switch (c) {
        case Retain r -> pos += r.count();
        case Insert i -> { if (pos <= cursor) shift += i.text().length(); }
        case Delete d -> {
          int overlap = Math.min(cursor, pos + d.count()) - pos;
          if (overlap > 0) shift -= overlap;
          pos += d.count();
        }
      }
    }
    return Math.max(0, result + shift);
  }

  static List<Component> normalize(List<Component> in) {
    List<Component> out = new ArrayList<>();
    for (Component c : in) {
      if (Cursor.len(c) == 0) continue;
      if (!out.isEmpty()) {
        Component l = out.get(out.size() - 1);
        if (l instanceof Retain a && c instanceof Retain b) { out.set(out.size() - 1, new Retain(a.count() + b.count())); continue; }
        if (l instanceof Delete a && c instanceof Delete b) { out.set(out.size() - 1, new Delete(a.count() + b.count())); continue; }
        if (l instanceof Insert a && c instanceof Insert b) { out.set(out.size() - 1, new Insert(a.text() + b.text())); continue; }
      }
      out.add(c);
    }
    return out;
  }

  private static final class Cursor {
    private final List<Component> cs;
    private int i;
    private int off;

    Cursor(List<Component> cs) { this.cs = cs; }

    Component peek() {
      if (i >= cs.size()) return null;
      return switch (cs.get(i)) {
        case Retain r -> new Retain(r.count() - off);
        case Delete d -> new Delete(d.count() - off);
        case Insert in -> in;
      };
    }

    static int len(Component c) {
      return switch (c) {
        case Retain r -> r.count();
        case Delete d -> d.count();
        case Insert in -> in.text().length();
      };
    }

    void skipInsert() { i++; off = 0; }

    void advance(int n) {
      if (off + n >= len(cs.get(i))) { i++; off = 0; } else { off += n; }
    }
  }
}
