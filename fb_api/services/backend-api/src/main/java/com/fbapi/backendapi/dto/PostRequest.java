package com.fbapi.backendapi.dto;

import lombok.Data;
import java.util.List;

@Data
public class PostRequest {
    private String message;
    private List<String> imageUrls;
}
