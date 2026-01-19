package vgu.cloud26;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;

public class LambdaDeleteOrchestrator implements RequestHandler<Map<String, Object>, APIGatewayProxyResponseEvent> {

    // ===== CONFIG =====
    private static final Region AWS_REGION = Region.AP_SOUTHEAST_2;

    private static final String ORIGINAL_BUCKET = "lmitu16";
    private static final String RESIZED_BUCKET  = "myresizedimagesbucket";
    private static final String RESIZED_PREFIX  = "resized-";

    private static final String RDS_INSTANCE_HOSTNAME = "database-1.cdq0ekg8q844.ap-southeast-2.rds.amazonaws.com";
    private static final int    RDS_INSTANCE_PORT     = 3306;
    private static final String DB_USER               = "cloud26";
    private static final String JDBC_URL =
            "jdbc:mysql://" + RDS_INSTANCE_HOSTNAME + ":" + RDS_INSTANCE_PORT + "/cloud26";

    private final S3Client s3Client = S3Client.builder()
            .region(AWS_REGION)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(Map<String, Object> input, Context context) {

        String method = getHttpMethod(input);
        if (method == null) method = "POST";

        // 1) Handle CORS preflight properly
        if ("OPTIONS".equalsIgnoreCase(method)) {
            return createCorsResponse(200, "");
        }

        context.getLogger().log("Delete Orchestrator started");

        try {
            // 2) Parse body safely
            String requestBody = extractBodyAsString(input);

            // Some invocations might send key directly (fallback)
            if ((requestBody == null || requestBody.isEmpty()) && input != null && input.containsKey("key")) {
                JSONObject fallback = new JSONObject();
                fallback.put("key", String.valueOf(input.get("key")));
                requestBody = fallback.toString();
            }

            if (requestBody == null || requestBody.isEmpty()) {
                return createCorsJsonResponse(400, new JSONObject().put("error", "Request body is missing").toString());
            }

            JSONObject inputJson = new JSONObject(requestBody);
            if (!inputJson.has("key")) {
                return createCorsJsonResponse(400, new JSONObject().put("error", "JSON body must contain 'key'").toString());
            }

            String key = inputJson.getString("key").trim();
            if (key.isEmpty()) {
                return createCorsJsonResponse(400, new JSONObject().put("error", "'key' must not be empty").toString());
            }

            context.getLogger().log("Deleting key: " + key);

            // 3) Delete DB row (IAM auth token)
            DbDeleteResult dbResult = deleteFromDatabaseByS3Key_IamAuth(key, context);

            // 4) Delete from S3 (original + resized)
            S3DeleteResult s3Original = deleteFromS3(ORIGINAL_BUCKET, key, context);
            S3DeleteResult s3Resized  = deleteFromS3(RESIZED_BUCKET, RESIZED_PREFIX + key, context);

            // 5) Response JSON
            JSONObject response = new JSONObject();
            response.put("success", true);
            response.put("message", "Delete completed");
            response.put("key", key);

            response.put("dbSuccess", dbResult.success);
            response.put("dbDeletedRows", dbResult.rowsDeleted);
            if (!dbResult.success) response.put("dbError", dbResult.errorMessage);

            response.put("s3OriginalSuccess", s3Original.success);
            response.put("s3OriginalMessage", s3Original.message);

            response.put("s3ResizedSuccess", s3Resized.success);
            response.put("s3ResizedMessage", s3Resized.message);

            return createCorsJsonResponse(200, response.toString());

        } catch (Exception e) {
            context.getLogger().log("Delete Orchestrator error: " + e.getClass().getName() + " - " + e.getMessage());
            JSONObject err = new JSONObject();
            err.put("error", e.getClass().getName() + ": " + safe(e.getMessage()));
            return createCorsJsonResponse(500, err.toString());
        }
    }

    // ===== CORS Responses =====
    private APIGatewayProxyResponseEvent createCorsResponse(int statusCode, String body) {
        APIGatewayProxyResponseEvent res = new APIGatewayProxyResponseEvent();
        res.setStatusCode(statusCode);
        res.setBody(body == null ? "" : body);
        res.setHeaders(corsHeaders(null));
        return res;
    }

    private APIGatewayProxyResponseEvent createCorsJsonResponse(int statusCode, String jsonBody) {
        APIGatewayProxyResponseEvent res = new APIGatewayProxyResponseEvent();
        res.setStatusCode(statusCode);
        res.setBody(jsonBody == null ? "{}" : jsonBody);
        res.setHeaders(corsHeaders("application/json"));
        return res;
    }

    private Map<String, String> corsHeaders(String contentType) {
        Map<String, String> h = new HashMap<String, String>();
        h.put("Access-Control-Allow-Origin", "*");
        h.put("Access-Control-Allow-Headers", "*");
        h.put("Access-Control-Allow-Methods", "OPTIONS,POST");
        if (contentType != null) h.put("Content-Type", contentType);
        return h;
    }

    // ===== Body extraction (Function URL can send base64) =====
    private String extractBodyAsString(Map<String, Object> input) {
        if (input == null) return null;

        Object bodyObj = input.get("body");
        if (bodyObj == null) return null;

        String body = String.valueOf(bodyObj);

        Object isB64Obj = input.get("isBase64Encoded");
        boolean isB64 = false;
        if (isB64Obj instanceof Boolean) isB64 = (Boolean) isB64Obj;
        if (isB64Obj instanceof String) isB64 = "true".equalsIgnoreCase((String) isB64Obj);

        if (!isB64) return body;

        byte[] decoded = Base64.getDecoder().decode(body);
        return new String(decoded, StandardCharsets.UTF_8);
    }

    // ===== HTTP method extraction (Function URL event schema) =====
    @SuppressWarnings("unchecked")
    private String getHttpMethod(Map<String, Object> input) {
        if (input == null) return null;

        // Sometimes present in REST API style
        Object httpMethod = input.get("httpMethod");
        if (httpMethod != null) return String.valueOf(httpMethod);

        // Function URL style: requestContext.http.method
        Object rcObj = input.get("requestContext");
        if (rcObj instanceof Map) {
            Map<String, Object> rc = (Map<String, Object>) rcObj;
            Object httpObj = rc.get("http");
            if (httpObj instanceof Map) {
                Map<String, Object> http = (Map<String, Object>) httpObj;
                Object methodObj = http.get("method");
                if (methodObj != null) return String.valueOf(methodObj);
            }
        }
        return null;
    }

    // ===== S3 delete =====
    private S3DeleteResult deleteFromS3(String bucket, String key, Context context) {
        try {
            context.getLogger().log("S3 delete: bucket=" + bucket + ", key=" + key);

            DeleteObjectRequest req = DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();

            s3Client.deleteObject(req);
            return S3DeleteResult.ok("Deleted " + key + " from " + bucket);

        } catch (Exception e) {
            context.getLogger().log("S3 delete FAILED: " + e.getClass().getName() + " - " + e.getMessage());
            return S3DeleteResult.fail(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    // ===== DB delete via IAM token (same pattern as your upload) =====
    private DbDeleteResult deleteFromDatabaseByS3Key_IamAuth(String s3Key, Context context) {
        context.getLogger().log("DB delete (IAM): starting for S3Key=" + s3Key);
        context.getLogger().log("DB url=" + JDBC_URL + ", user=" + DB_USER + ", region=" + AWS_REGION);

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            Properties props = new Properties();
            props.setProperty("useSSL", "true");
            props.setProperty("user", DB_USER);
            props.setProperty("password", generateAuthToken());

            try (Connection connection = DriverManager.getConnection(JDBC_URL, props)) {
                String sql = "DELETE FROM Photos WHERE S3Key = ?";

                try (PreparedStatement st = connection.prepareStatement(sql)) {
                    st.setString(1, s3Key);
                    int rows = st.executeUpdate();
                    context.getLogger().log("DB delete OK, rows affected=" + rows);
                    return DbDeleteResult.ok(rows);
                }
            }

        } catch (Exception e) {
            context.getLogger().log("DB delete FAILED: " + e.getClass().getName() + " - " + e.getMessage());
            return DbDeleteResult.fail(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private String generateAuthToken() throws Exception {
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

    private String safe(String s) {
        return s == null ? "" : s.replace("\"", "'");
    }

    // ===== Result wrapper =====
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

    private static class S3DeleteResult {
        final boolean success;
        final String message;

        private S3DeleteResult(boolean success, String message) {
            this.success = success;
            this.message = message;
        }

        static S3DeleteResult ok(String msg) {
            return new S3DeleteResult(true, msg);
        }

        static S3DeleteResult fail(String msg) {
            return new S3DeleteResult(false, msg);
        }
    }
}
