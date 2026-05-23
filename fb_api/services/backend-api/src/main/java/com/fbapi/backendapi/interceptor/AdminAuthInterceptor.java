package com.fbapi.backendapi.interceptor;

import com.fbapi.backendapi.exception.UnauthorizedException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class AdminAuthInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String role = request.getHeader("X-User-Role");
        if (role == null || !role.equals("ADMIN")) {
            throw new UnauthorizedException("Access Denied: Only Admins can perform this action.");
        }
        return true;
    }
}
