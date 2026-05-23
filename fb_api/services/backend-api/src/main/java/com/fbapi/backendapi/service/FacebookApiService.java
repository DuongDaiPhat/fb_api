package com.fbapi.backendapi.service;

import com.fbapi.backendapi.dto.PostRequest;
import com.fbapi.backendapi.exception.FacebookApiException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class FacebookApiService {

    private final WebClient webClient;
    private final String pageAccessToken;
    private final String pageId;

    public FacebookApiService(WebClient.Builder webClientBuilder,
            @Value("${facebook.graph-api-url}") String graphApiUrl,
            @Value("${facebook.page-access-token}") String pageAccessToken,
            @Value("${facebook.page-id}") String pageId) {
        this.webClient = webClientBuilder.baseUrl(graphApiUrl).build();
        this.pageAccessToken = pageAccessToken;
        this.pageId = pageId;
    }

    @CircuitBreaker(name = "facebookApi", fallbackMethod = "fallbackGetPosts")
    @Retry(name = "facebookApi")
    public Object getPosts() {
        log.info("Sending request to Facebook API to get posts for page {}", pageId);
        try {
            return webClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/{page-id}/posts")
                            .queryParam("access_token", pageAccessToken)
                            .build(pageId))
                    .retrieve()
                    .bodyToMono(Object.class)
                    .doOnSuccess(response -> log.info("Successfully retrieved posts from Facebook"))
                    .block();
        } catch (WebClientResponseException e) {
            handleException(e);
            return null;
        }
    }

    @CircuitBreaker(name = "facebookApi", fallbackMethod = "fallbackCreatePost")
    @Retry(name = "facebookApi")
    public Object createPost(PostRequest request) {
        log.info("Sending request to Facebook API to create post. Content length: {}",
                request.getMessage() != null ? request.getMessage().length() : 0);

        try {
            if (request.getImageUrls() != null && !request.getImageUrls().isEmpty()) {
                List<String> mediaIds = new ArrayList<>();
                for (String imageUrl : request.getImageUrls()) {
                    Map<String, Object> photoVars = Map.of(
                            "pageId", pageId,
                            "token", pageAccessToken,
                            "url", imageUrl);
                    Map response = webClient.post()
                            .uri(uriBuilder -> uriBuilder
                                    .path("/{pageId}/photos")
                                    .queryParam("access_token", "{token}")
                                    .queryParam("url", "{url}")
                                    .queryParam("published", "false")
                                    .build(photoVars))
                            .retrieve()
                            .bodyToMono(Map.class)
                            .block();
                    if (response != null && response.containsKey("id")) {
                        mediaIds.add(response.get("id").toString());
                    }
                }

                StringBuilder attachedMediaJson = new StringBuilder("[");
                for (int i = 0; i < mediaIds.size(); i++) {
                    attachedMediaJson.append("{\"media_fbid\":\"").append(mediaIds.get(i)).append("\"}");
                    if (i < mediaIds.size() - 1)
                        attachedMediaJson.append(",");
                }
                attachedMediaJson.append("]");

                Map<String, Object> feedVars = Map.of(
                        "pageId", pageId,
                        "token", pageAccessToken,
                        "msg", request.getMessage() != null ? request.getMessage() : "",
                        "media", attachedMediaJson.toString());

                return webClient.post()
                        .uri(uriBuilder -> uriBuilder
                                .path("/{pageId}/feed")
                                .queryParam("access_token", "{token}")
                                .queryParam("message", "{msg}")
                                .queryParam("attached_media", "{media}")
                                .build(feedVars))
                        .retrieve()
                        .bodyToMono(Object.class)
                        .doOnSuccess(res -> log.info("Successfully created post with images"))
                        .block();
            } else {
                Map<String, Object> feedVars = Map.of(
                        "pageId", pageId,
                        "token", pageAccessToken,
                        "msg", request.getMessage() != null ? request.getMessage() : "");
                return webClient.post()
                        .uri(uriBuilder -> uriBuilder
                                .path("/{pageId}/feed")
                                .queryParam("access_token", "{token}")
                                .queryParam("message", "{msg}")
                                .build(feedVars))
                        .retrieve()
                        .bodyToMono(Object.class)
                        .doOnSuccess(res -> log.info("Successfully created post"))
                        .block();
            }
        } catch (WebClientResponseException e) {
            handleException(e);
            return null;
        }
    }

    @CircuitBreaker(name = "facebookApi", fallbackMethod = "fallbackGetComments")
    @Retry(name = "facebookApi")
    public Object getComments(String postId) {
        log.info("Sending request to Facebook API to get comments for post {}", postId);
        try {
            return webClient.get()
                    .uri(uriBuilder -> uriBuilder
                            .path("/{post-id}/comments")
                            .queryParam("access_token", pageAccessToken)
                            .build(postId))
                    .retrieve()
                    .bodyToMono(Object.class)
                    .doOnSuccess(response -> log.info("Successfully retrieved comments"))
                    .block();
        } catch (WebClientResponseException e) {
            handleException(e);
            return null;
        }
    }

    private void handleException(WebClientResponseException e) {
        log.error("Facebook API Error: Status {}, Response {}", e.getStatusCode(), e.getResponseBodyAsString());
        String errorCode = String.valueOf(e.getStatusCode().value());
        String message = "Error communicating with Facebook API";
        if (e.getStatusCode().value() == 401) {
            message = "Unauthorized: Page Access Token may be expired or invalid.";
        } else if (e.getStatusCode().value() == 403) {
            message = "Forbidden: App does not have required permissions.";
        }
        throw new FacebookApiException(message, errorCode);
    }

    public Object fallbackGetPosts(Throwable t) {
        log.error("Fallback triggered for getPosts: {}", t.getMessage());
        throw new FacebookApiException("Facebook service is temporarily unavailable. Please try again later.", "503");
    }

    public Object fallbackCreatePost(PostRequest request, Throwable t) {
        log.error("Fallback triggered for createPost: {}", t.getMessage());
        throw new FacebookApiException("Facebook service is temporarily unavailable. Please try again later.", "503");
    }

    public Object fallbackGetComments(String postId, Throwable t) {
        log.error("Fallback triggered for getComments: {}", t.getMessage());
        throw new FacebookApiException("Facebook service is temporarily unavailable. Please try again later.", "503");
    }
}
