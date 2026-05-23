package com.fbapi.backendapi.controller;

import com.fbapi.backendapi.dto.ApiResponse;
import com.fbapi.backendapi.dto.PostRequest;
import com.fbapi.backendapi.service.FacebookApiService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Tag(name = "Facebook API", description = "Endpoints interacting with Facebook Graph API")
public class FacebookController {

    private final FacebookApiService facebookApiService;

    @GetMapping("/posts")
    @Operation(summary = "Get list of posts from Facebook Page")
    public ResponseEntity<ApiResponse<Object>> getPosts() {
        Object posts = facebookApiService.getPosts();
        return ResponseEntity.ok(ApiResponse.success(posts, "Fetched posts successfully"));
    }

    @PostMapping("/posts")
    @Operation(summary = "Create a new post on Facebook Page (supports multiple images)")
    public ResponseEntity<ApiResponse<Object>> createPost(@RequestBody PostRequest request) {
        Object response = facebookApiService.createPost(request);
        return ResponseEntity.ok(ApiResponse.success(response, "Post created successfully"));
    }

    @GetMapping("/comments")
    @Operation(summary = "Get comments of a specific post")
    public ResponseEntity<ApiResponse<Object>> getComments(@RequestParam("postId") String postId) {
        Object comments = facebookApiService.getComments(postId);
        return ResponseEntity.ok(ApiResponse.success(comments, "Fetched comments successfully"));
    }
}
