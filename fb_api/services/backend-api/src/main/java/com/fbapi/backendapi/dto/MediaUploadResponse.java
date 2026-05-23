package com.fbapi.backendapi.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class MediaUploadResponse {
    private String url;
    private String publicId;
}
