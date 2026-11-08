package com.mk.mq.dashboard;

import com.mk.mq.*;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** 仅公开固定资源及 GET 查询，不通过看板执行恢复、重放、删除或发送。 */
@RestController
@RequestMapping(DashboardController.BASE)
public final class DashboardController {
    public static final String BASE = "/redis-mq/dashboard";
    private static final Logger log = LoggerFactory.getLogger(DashboardController.class);
    private final RedisMq mq;
    DashboardController(RedisMq mq) { this.mq = mq; }

    @GetMapping("")
    public ResponseEntity<Void> redirect(HttpServletRequest request) {
        return ResponseEntity.status(HttpStatus.FOUND).location(URI.create(request.getContextPath() + BASE + "/")).build();
    }
    @GetMapping(value = "/", produces = MediaType.TEXT_HTML_VALUE)
    public ResponseEntity<Resource> index() { return resource("index.html", MediaType.TEXT_HTML); }
    @GetMapping(value = "/dashboard.css", produces = "text/css")
    public ResponseEntity<Resource> styles() { return resource("dashboard.css", MediaType.parseMediaType("text/css")); }
    @GetMapping(value = "/dashboard.js", produces = "text/javascript")
    public ResponseEntity<Resource> script() { return resource("dashboard.js", MediaType.parseMediaType("text/javascript")); }

    @GetMapping("/api/overview")
    public Overview overview(@RequestParam(defaultValue = "0") int offset, @RequestParam(defaultValue = "20") int count) {
        page(offset, count);
        List<String> names = mq.registeredQueues(offset, count);
        List<QueueView> queues = new ArrayList<>();
        for (String queue : names) {
            try { queues.add(new QueueView(queue, mq.stats(queue), null)); }
            catch (RuntimeException e) {
                log.warn("看板读取队列 {} 状态失败", queue, e);
                queues.add(new QueueView(queue, null, "暂时无法读取此队列"));
            }
        }
        return new Overview(mq.namespace(), Instant.now().toString(), offset, count, mq.registeredQueueCount(), List.copyOf(queues));
    }

    @GetMapping("/api/queues/{queue}/messages")
    public List<MessageSummary> messages(@PathVariable String queue, @RequestParam(defaultValue = "SCHEDULED") MessageState state,
                                         @RequestParam(defaultValue = "0") int offset, @RequestParam(defaultValue = "20") int count) {
        page(offset, count);
        return mq.messages(queue, state, offset, count);
    }

    @GetMapping("/api/queues/{queue}/messages/{id}")
    public MessageSnapshot detail(@PathVariable String queue, @PathVariable String id) {
        MessageSnapshot message = mq.get(queue, id);
        if (message == null) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "消息已完成、取消或被删除");
        return message;
    }

    @ExceptionHandler({IllegalArgumentException.class, MethodArgumentTypeMismatchException.class})
    public ResponseEntity<Map<String, String>> badRequest() {
        return ResponseEntity.badRequest().cacheControl(CacheControl.noStore()).body(Map.of("message", "查询参数不合法"));
    }
    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Map<String, String>> missing(ResponseStatusException exception) {
        return ResponseEntity.status(exception.getStatusCode()).cacheControl(CacheControl.noStore())
                .body(Map.of("message", exception.getReason() == null ? "请求无法完成" : exception.getReason()));
    }
    @ExceptionHandler(RuntimeException.class)
    public ResponseEntity<Map<String, String>> unavailable(RuntimeException exception) {
        log.warn("MQ 看板查询失败", exception);
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).cacheControl(CacheControl.noStore())
                .body(Map.of("message", "暂时无法读取 Redis，请稍后重试"));
    }
    private static void page(int offset, int count) {
        if (offset < 0 || count < 1 || count > 100) throw new IllegalArgumentException("分页范围不合法");
    }
    private static ResponseEntity<Resource> resource(String name, MediaType type) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).contentType(type)
                .body(new ClassPathResource("com/mk/mq/dashboard/" + name));
    }
    public record Overview(String namespace, String capturedAt, int offset, int count, int totalQueues, List<QueueView> queues) { }
    public record QueueView(String name, QueueStats stats, String error) { }
}
