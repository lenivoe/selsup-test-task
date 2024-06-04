package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class CrptApi {
    // пример использования
    public static void main(String[] args) {
        var crptApi = CrptApi.create(Duration.of(1, ChronoUnit.SECONDS), 5);

        var docs = Stream.generate(CrptApi::generateDocument)
                .limit(40)
                .toList();

        var futures = docs.stream()
                .map(doc -> CompletableFuture.runAsync(() -> {
                    try {
                        var response = crptApi.createDocument(doc, "token");
                        System.out.println("doc id: " + doc.docId + "\n" +
                                "response: " + response);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (ApiException e) {
                        System.err.println("doc id: " + doc.docId + "\n"
                                + "status code: " + e.getStatusCode() + "\n"
                                + "error: " + e.getMessage());
                    }
                }))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
    }

    private final RateLimiter rateLimiter;
    private final HttpClient httpClient = HttpClient.newHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public CrptApi(RateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    public static CrptApi create(Duration interval, int requestLimit) {
        return new CrptApi(new RateLimiter(interval, requestLimit));
    }

    // Из задания не очевидно, что подразумевается под подписью.
    // Предполагаю, что это токен аутентификации, например jwt.
    public String createDocument(Document document, String jwtToken) throws ApiException, InterruptedException {
        rateLimiter.acquire();
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI("https://ismp.crpt.ru/api/v3/lk/documents/create"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + jwtToken)
                    .POST(BodyPublishers.ofString(objectMapper.writeValueAsString(document)))
                    .build();

            HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());

            if (response.statusCode() != 200) {
                throw new ApiException(response.statusCode(), "Failed to create document: " + response.body());
            }

            return response.body();
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    // К данному классу есть тест в папке с тестами
    /**
     * Ограничивает выполнение запросов в заданный интервал времени.
     */
    public static class RateLimiter {
        private final long intervalNanos;
        private final int requestLimit;
        private final Queue<Long> requestTimestamps;

        /**
         * Создает ограничитель запросов
         * @param interval     указывает промежуток времени – секунда, минута и пр.
         * @param requestLimit положительное значение, которое определяет максимальное количество запросов в этом промежутке времени.
         */
        public RateLimiter(Duration interval, int requestLimit) {
            if (requestLimit <= 0) {
                throw new IllegalArgumentException("requestLimit must be positive");
            }
            this.intervalNanos = interval.toNanos();
            this.requestLimit = requestLimit;
            this.requestTimestamps = new ArrayDeque<>(requestLimit);
        }

        /**
         * Занять место под новый запрос в заданном временном интервале.<br>
         * Блокирует поток, пока исчерпан лимит запросов на интервал.<br>
         * Как только становится возможной отправка нового запроса,
         * выбирается один из ожидающих либо первый встречный запрос, если ожидающих нет.<br>
         * Порядок выбора запросов неопределен.
         * @throws InterruptedException выбрасывается в момент прерывания потока
         */
        @SuppressWarnings({"BusyWait", "DataFlowIssue"})
        public synchronized void acquire() throws InterruptedException {
            long timestamp;
            while (true) {
                timestamp = System.nanoTime();
                while (!requestTimestamps.isEmpty() && (timestamp - requestTimestamps.peek()) > intervalNanos) {
                    requestTimestamps.poll();
                }
                if (requestTimestamps.size() < requestLimit) {
                    break;
                }
                long waitTimeNanos = intervalNanos - (timestamp - requestTimestamps.peek());
                Thread.sleep(waitTimeNanos / 1_000_000, (int) (waitTimeNanos % 1_000_000));
            }
            requestTimestamps.offer(timestamp);
        }
    }

    public static class ApiException extends Exception {
        private final int statusCode;

        public ApiException(int statusCode, String message) {
            super(message);
            this.statusCode = statusCode;
        }

        public ApiException(int statusCode, String message, Throwable cause) {
            super(message, cause);
            this.statusCode = statusCode;
        }

        public int getStatusCode() {
            return statusCode;
        }
    }

    // для компактности геттеры, сеттеры и конструкторы отсутсвуют, а все поля публичные
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public static class Document {
        public Description description;
        public String docId;
        public String docStatus;
        public String docType = "LP_INTRODUCE_GOODS";
        @JsonProperty("importRequest")
        public boolean importRequest;
        public String ownerInn;
        public String participantInn;
        public String producerInn;
        public String productionDate;
        public String productionType;
        public List<Product> products;
        public String regDate;
        public String regNumber;

        public static class Description {
            @JsonProperty("participantInn")
            public String participantInn;
        }

        @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
        public static class Product {
            public String certificateDocument;
            public String certificateDocumentDate;
            public String certificateDocumentNumber;
            public String ownerInn;
            public String producerInn;
            public String productionDate;
            public String tnvedCode;
            public String uitCode;
            public String uituCode;
        }
    }

    private static CrptApi.Document generateDocument() {
        var doc = new CrptApi.Document();
        doc.description = new CrptApi.Document.Description();
        doc.docId = UUID.randomUUID().toString();
        doc.docStatus = "creating";
        doc.importRequest = true;
        doc.ownerInn = "1234";
        doc.participantInn = "000000000000";
        doc.producerInn = "000000000000";
        doc.productionDate = "2020-01-23";
        doc.productionType = "type";
        doc.products = List.of(new CrptApi.Document.Product());
        doc.regDate = "2020-01-23";
        doc.regNumber = "1";

        doc.description.participantInn = "000000000000";

        doc.products.get(0).certificateDocument = "doc";
        doc.products.get(0).certificateDocumentDate = "2020-01-23";
        doc.products.get(0).certificateDocumentNumber = "2";
        doc.products.get(0).ownerInn = "000000000000";
        doc.products.get(0).producerInn = "000000000000";
        doc.products.get(0).productionDate = "2020-01-23";
        doc.products.get(0).tnvedCode = "3";
        doc.products.get(0).uitCode = "4";
        doc.products.get(0).uituCode = "5";

        return doc;
    }
}
