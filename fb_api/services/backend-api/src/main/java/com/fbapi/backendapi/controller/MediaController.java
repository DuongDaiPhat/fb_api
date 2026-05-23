package com.fbapi.backendapi.controller;

import com.fbapi.backendapi.dto.ApiResponse;
import com.fbapi.backendapi.dto.MediaUploadResponse;
import com.fbapi.backendapi.service.CloudinaryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@RequestMapping("/api/media")
@RequiredArgsConstructor
@Tag(name = "Media", description = "Media Upload API")
public class MediaController {

    private final CloudinaryService cloudinaryService;

    @PostMapping("/uploads")
    @Operation(summary = "Upload image to Cloudinary")
    public ResponseEntity<ApiResponse<MediaUploadResponse>> uploadImage(@RequestParam("file") MultipartFile file) throws IOException {
        MediaUploadResponse response = cloudinaryService.uploadImage(file);
        return ResponseEntity.ok(ApiResponse.success(response, "Image uploaded successfully"));
    }
}
