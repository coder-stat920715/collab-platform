package com.acme.collab.realtime.adapters.in.rest;

import com.acme.collab.realtime.application.Exceptions.ForbiddenException;
import com.acme.collab.realtime.application.Exceptions.NotOwnerException;
import com.acme.collab.realtime.application.Exceptions.StaleRevisionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ErrorAdvice {

  @ExceptionHandler(ForbiddenException.class)
  ResponseEntity<ProblemDetail> forbidden(ForbiddenException e) {
    return problem(HttpStatus.FORBIDDEN, e);
  }

  @ExceptionHandler(StaleRevisionException.class)
  ResponseEntity<ProblemDetail> stale(StaleRevisionException e) {
    return problem(HttpStatus.CONFLICT, e);
  }

  @ExceptionHandler(NotOwnerException.class)
  ResponseEntity<ProblemDetail> notOwner(NotOwnerException e) {
    return problem(HttpStatusCode.valueOf(421), e);   // 421 Misdirected Request
  }

  @ExceptionHandler(Exception.class)
  ResponseEntity<ProblemDetail> other(Exception e) {
    return problem(HttpStatus.INTERNAL_SERVER_ERROR, e);
  }

  private ResponseEntity<ProblemDetail> problem(HttpStatusCode status, Exception e) {
    return ResponseEntity.status(status).body(ProblemDetail.forStatusAndDetail(status, e.getMessage()));
  }
}