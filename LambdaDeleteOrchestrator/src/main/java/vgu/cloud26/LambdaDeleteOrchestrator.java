package vgu.cloud26;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;

import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

public class LambdaDeleteOrchestrator implements RequestHandler<Map<String, Object>, APIGatewayProxyResponseEvent> {

    // --- AWS / BUCKETS ---
    private static final Region AWS_REGION = Region.AP_SOUTHEAST_2;
    private static final String ORIGINAL_BUCKET = "lmitu16";
    private static final String RESIZED_BUCKET = "myresizedimagesbucket";

    // --- DB CONFIG (match your Upload Lambda exactly) ---
    private static final String RDS_INSTANCE_HOSTNAME = "database-1.cdq0ekg8q844.ap-southeast-2.rds.amazonaws.com";
    private static final int RDS_INSTANCE_PORT = 3306;
    private static final String DB_USER = "cloud26";
    private static final String JDBC_URL =
            "jdbc:mysql://" + RDS_INSTANCE_HOSTNAME + ":" + RDS_INSTANCE_PORT + "/cloud26";

    private final S3Client s3Client = S3Client.builder()
            .region(AWS_REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .httpClientBuilder(
                    UrlConnectionHttpClient.builder()
                            .connectionTimeout(Duration.ofSeconds(5))
                            .socketTimeout(Duration.ofSeconds(10))
            )
            .overrideConfiguration(
                    ClientOverrideConfiguration.builder()
                            .apiCallTimeout(Duration.ofSeconds(15))
                            .apiCallAttemptTimeout(Duration.ofSeconds(15))
                            .build()
            )
            .build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(Map<String, Object> input, Context context) {
        context.getLogger().log("Delete Orchestrator started");

        // Handle OPTIONS (CORS preflight) defensively
        String method = extractHttpMethod(input);
        if ("OPTIONS".equalsIgnoreCase(method)) {
            return createResponse(200, "{\"ok\":true}");
        }

        try {
            // --- 1) Parse body ---
            String requestBody = (String) input.get("body");

            if (requestBody == null || requestBody.isEmpty()) {
                if (input.containsKey("key")) {
                    requestBody = new JSONObject(input).toString();
                } else {
                    return createResponse(400, "{\"error\":\"Request body is missing\"}");
                }
            }

            JSONObject inputJson = new JSONObject(requestBody);
            if (!inputJson.has("key")) {
                return createResponse(400, "{\"error\":\"JSON body must contain 'key'\"}");
            }

            String key = inputJson.getString("key");
            context.getLogger().log("Deleting key: " + key);

            // --- 2) Delete DB row FIRST (so you can still debug even if S3 fails) ---
            DbDeleteResult dbResult = deleteFromDatabaseByS3Key_IamAuth(key, context);

            // --- 3) Delete S3 objects (idempotent, delete works even if object doesn't exist) ---
            String originalKey = key;
            String resizedKey = "resized-" + key;

            String s3OriginalStatus = deleteS3Object(ORIGINAL_BUCKET, originalKey, context);
            String s3ResizedStatus = deleteS3Object(RESIZED_BUCKET, resizedKey, context);

            // --- 4) Response ---
            JSONObject response = new JSONObject();
            response.put("success", true);
            response.put("message", "Delete completed");
            response.put("key", key);

            response.put("dbSuccess", dbResult.success);
            response.put("dbDeletedRows", dbResult.rowsDeleted);
            if (!dbResult.success) response.put("dbError", dbResult.errorMessage);

            response.put("s3Original", s3OriginalStatus);
            response.put("s3Resized", s3ResizedStatus);

            return createResponse(200, response.toString());

        } catch (Exception e) {
            context.getLogger().log("Delete Orchestrator error: " + e.getClass().getName() + " - " + e.getMessage());
            return createResponse(500, "{\"error\":\"" + escapeJson(e.getMessage()) + "\"}");
        }
    }

    private String deleteS3Object(String bucket, String key, Context context) {
        try {
            context.getLogger().log("S3 delete: bucket=" + bucket + ", key=" + key);
            DeleteObjectRequest req = DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            s3Client.deleteObject(req);
            return "deleted";
        } catch (Exception e) {
            context.getLogger().log("S3 delete FAILED for " + bucket + "/" + key + ": " + e.getMessage());
            return "error: " + e.getMessage();
        }
    }

    // --- DB delete via IAM token (same pattern as Upload Lambda) ---
    private DbDeleteResult deleteFromDatabaseByS3Key_IamAuth(String s3Key, Context context) {
        context.getLogger().log("DB delete (IAM): starting for S3Key=" + s3Key);
        context.getLogger().log("DB url=" + JDBC_URL + ", user=" + DB_USER + ", region=" + AWS_REGION);

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            Properties props = buildMysqlPropsIamToken();

            try (Connection connection = DriverManager.getConnection(JDBC_URL, props)) {
                String sql = "DELETE FROM Photos WHERE S3Key = ?";

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, s3Key);
                    int rows = statement.executeUpdate();
                    context.getLogger().log("DB delete OK, rows affected=" + rows);
                    return DbDeleteResult.ok(rows);
                }
            }

        } catch (Exception e) {
            context.getLogger().log("DB delete FAILED: " + e.getClass().getName() + " - " + e.getMessage());
            return DbDeleteResult.fail(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private static Properties buildMysqlPropsIamToken() throws Exception {
        Properties props = new Properties();
        props.setProperty("useSSL", "true");
        props.setProperty("user", DB_USER);
        props.setProperty("password", generateAuthToken());

        props.setProperty("serverTimezone", "UTC");
        props.setProperty("useUnicode", "true");
        props.setProperty("characterEncoding", "UTF-8");
        return props;
    }

    private static String generateAuthToken() throws Exception {
        RdsUtilities rdsUtilities = RdsUtilities.builder().build();
        return rdsUtilities.generateAuthenticationToken(
                GenerateAuthenticationTokenRequest.builder()
                        .hostname(RDS_INSTANCE_HOSTNAME)
                        .port(RDS_INSTANCE_PORT)
                        .username(DB_USER)
                        .region(AWS_REGION)
                        .credentialsProvider(DefaultCredentialsProvider.create())
                        .build()
        );
    }

    private APIGatewayProxyResponseEvent createResponse(int statusCode, String body) {
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Access-Control-Allow-Headers", "*");
        headers.put("Access-Control-Allow-Methods", "OPTIONS,POST");

        return new APIGatewayProxyResponseEvent()
                .withStatusCode(statusCode)
                .withHeaders(headers)
                .withBody(body);
    }

    private String extractHttpMethod(Map<String, Object> input) {
        try {
            Object rcObj = input.get("requestContext");
            if (!(rcObj instanceof Map)) return null;

            Map<?, ?> rc = (Map<?, ?>) rcObj;
            Object httpObj = rc.get("http");
            if (!(httpObj instanceof Map)) return null;

            Map<?, ?> http = (Map<?, ?>) httpObj;
            Object methodObj = http.get("method");
            return (methodObj instanceof String) ? (String) methodObj : null;
        } catch (Exception e) {
            return null;
        }
    }

    private String escapeJson(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static class DbDeleteResult {
        final boolean success;
        final int rowsDeleted;
        final String errorMessage;

        private DbDeleteResult(boolean success, int rowsDeleted, String errorMessage) {
            this.success = success;
            this.rowsDeleted = rowsDeleted;
            this.errorMessage = errorMessage;
        }

        static DbDeleteResult ok(int rows) {
            return new DbDeleteResult(true, rows, null);
        }

        static DbDeleteResult fail(String msg) {
            return new DbDeleteResult(false, 0, msg);
        }
    }
}
