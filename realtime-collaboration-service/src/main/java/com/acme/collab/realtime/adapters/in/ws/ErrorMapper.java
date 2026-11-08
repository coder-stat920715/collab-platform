package com.acme.collab.realtime.adapters.in.ws;

import com.acme.collab.common.Frame;
import com.acme.collab.realtime.application.Exceptions.BadFrameException;
import com.acme.collab.realtime.application.Exceptions.ForbiddenException;
import com.acme.collab.realtime.application.Exceptions.InvalidOperationException;
import com.acme.collab.realtime.application.Exceptions.NotOwnerException;
import com.acme.collab.realtime.application.Exceptions.SlowConsumerException;
import com.acme.collab.realtime.application.Exceptions.StaleRevisionException;
import java.util.Map;

/** WebSocket frames do not pass through MVC advice, so exceptions are mapped to ERROR frames here. */
public final class ErrorMapper {
  private ErrorMapper() {}

  public static Frame toFrame(Throwable e) {
    return switch (e) {
      case ForbiddenException f -> err("FORBIDDEN", false, f.getMessage());
      case StaleRevisionException s -> err("RESYNC", true, s.getMessage());
      case InvalidOperationException i -> err("INVALID_OP", false, i.getMessage());
      case BadFrameException b -> err("BAD_FRAME", false, b.getMessage());
      case SlowConsumerException x -> err("SLOW_CONSUMER", true, x.getMessage());
      case NotOwnerException n ->
          new Frame("ERROR", Map.of("code", "NOT_OWNER", "retry", true, "owner", n.ownerNode));
      default -> err("INTERNAL", true, "internal error");
    };
  }

  private static Frame err(String code, boolean retry, String message) {
    return new Frame("ERROR", Map.of("code", code, "retry", retry, "message", String.valueOf(message)));
  }
}
