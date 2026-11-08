package com.acme.collab.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.acme.collab.common.Component.Delete;
import com.acme.collab.common.Component.Insert;
import com.acme.collab.common.Component.Retain;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;

class OtEngineTest {

  @Test
  void concurrentInsertsAtSamePositionAreDeterministic() {
    List<Component> a = List.of(new Insert("A"), new Retain(3));
    List<Component> b = List.of(new Insert("B"), new Retain(3));
    var t = OtEngine.transform(a, b);
    String viaA = OtEngine.apply(OtEngine.apply("xyz", a), t.b());
    String viaB = OtEngine.apply(OtEngine.apply("xyz", b), t.a());
    assertEquals("ABxyz", viaA);
    assertEquals(viaA, viaB);
  }

  @Test
  void deleteVsInsertConverges() {
    List<Component> a = List.of(new Retain(1), new Delete(2));
    List<Component> b = List.of(new Retain(2), new Insert("Q"), new Retain(1));
    var t = OtEngine.transform(a, b);
    assertEquals(OtEngine.apply(OtEngine.apply("abc", a), t.b()),
                 OtEngine.apply(OtEngine.apply("abc", b), t.a()));
  }

  @Test
  void randomizedConvergenceTP1() {
    Random rnd = new Random(42);
    for (int n = 0; n < 50_000; n++) {
      String doc = randomText(rnd, rnd.nextInt(30));
      List<Component> a = randomOp(rnd, doc.length());
      List<Component> b = randomOp(rnd, doc.length());
      var t = OtEngine.transform(a, b);
      String viaA = OtEngine.apply(OtEngine.apply(doc, a), t.b());
      String viaB = OtEngine.apply(OtEngine.apply(doc, b), t.a());
      assertEquals(viaA, viaB, "doc=" + doc + " a=" + a + " b=" + b);
    }
  }

  @Test
  void cursorShiftsAcrossInsertAndDelete() {
    assertEquals(5, OtEngine.transformCursor(3, List.of(new Insert("ab"), new Retain(10))));
    assertEquals(1, OtEngine.transformCursor(3, List.of(new Retain(1), new Delete(2), new Retain(7))));
  }

  static List<Component> randomOp(Random rnd, int len) {
    List<Component> ops = new ArrayList<>();
    int remaining = len;
    while (remaining > 0) {
      int k = 1 + rnd.nextInt(remaining);
      switch (rnd.nextInt(3)) {
        case 0 -> ops.add(new Retain(k));
        case 1 -> ops.add(new Delete(k));
        default -> { ops.add(new Insert(randomText(rnd, 1 + rnd.nextInt(3)))); continue; }
      }
      remaining -= k;
    }
    if (rnd.nextBoolean()) ops.add(new Insert(randomText(rnd, 1 + rnd.nextInt(3))));
    return OtEngine.normalize(ops);
  }

  static String randomText(Random rnd, int n) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < n; i++) sb.append((char) ('a' + rnd.nextInt(26)));
    return sb.toString();
  }
}
