package com.learnkafkastreams.exceptionhandler;

import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalErrorHandler {

  @ExceptionHandler(IllegalStateException.class)
  public ProblemDetail handleIllegalStateException(IllegalStateException e) {
    var pd = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(400), e.getMessage());
    pd.setProperty("additionalInfo", "Please send a valid order type: general_orders or restaurant_orders");
    return pd;
  }
}
