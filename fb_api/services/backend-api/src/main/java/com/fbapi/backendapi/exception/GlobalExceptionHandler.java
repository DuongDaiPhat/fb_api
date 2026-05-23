package com.fbapi.backendapi.exception;

import com.fbapi.backendapi.dto.ApiResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(FacebookApiException.class)
    public ResponseEntity<ApiResponse<Void>> handleFacebookApiException(FacebookApiException ex) {
        // e.g. token expired, invalid permission etc.
        HttpStatus status = ex.getErrorCode().equals("401") ? HttpStatus.UNAUTHORIZED : HttpStatus.BAD_REQUEST;
        return new ResponseEntity<>(ApiResponse.error(ex.getMessage(), ex.getErrorCode()), status);
    }

    @ExceptionHandler(UnauthorizedException.class)
    public ResponseEntity<ApiResponse<Void>> handleUnauthorizedException(UnauthorizedException ex) {
        return new ResponseEntity<>(ApiResponse.error(ex.getMessage(), "401_UNAUTHORIZED"), HttpStatus.UNAUTHORIZED);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> handleGenericException(Exception ex) {
        return new ResponseEntity<>(ApiResponse.error("Internal Server Error: " + ex.getMessage(), "500_INTERNAL_ERROR"), HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
