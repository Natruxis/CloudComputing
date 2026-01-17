package vgu.cloud26;

import java.util.Map;

import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;

public class LambdaDeleteOrchestrator implements RequestHandler<Map<String, Object>, APIGatewayProxyResponseEvent> {

    private final LambdaClient lambdaClient = LambdaClient.builder()
            .region(Region.AP_SOUTHEAST_2)
            .build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(Map<String, Object> input, Context context) {
        context.getLogger().log("Delete Orchestrator gestartet");
        
        try {
            String requestBody = (String) input.get("body");
            
            // Fallback: Wenn body null ist, versuchen wir es direkt aus dem Input
            if (requestBody == null || requestBody.isEmpty()) {
                if (input.containsKey("key")) {
                   requestBody = new JSONObject(input).toString();
                } else {
                   return createResponse(400, "{\"error\": \"Request body is missing\"}");
                }
            }
            
            JSONObject inputJson = new JSONObject(requestBody);
            if (!inputJson.has("key")) {
                return createResponse(400, "{\"error\": \"JSON body must contain 'key'\"}");
            }
            
            String key = inputJson.getString("key");
            context.getLogger().log("Lösche Datei: " + key);
            
            // 1. Original löschen
            JSONObject deleteOriginal = new JSONObject();
            deleteOriginal.put("bucket", "lmitu16");
            deleteOriginal.put("key", key);
            String originalResult = invokeLambda("LambdaDeleteObject", deleteOriginal.toString());
            
            // 2. Thumbnail löschen
            JSONObject deleteResized = new JSONObject();
            deleteResized.put("bucket", "myresizedimagesbucket");
            deleteResized.put("key", "resized-" + key);
            String resizedResult = invokeLambda("LambdaDeleteObject", deleteResized.toString());
            
            // JSON Antwort bauen (Manuell, wie es vorher war)
            String body = String.format("{\"success\": true, \"message\": \"Deleted\", \"original\": \"%s\", \"resized\": \"%s\"}", 
                originalResult.replace("\"", "'"), resizedResult.replace("\"", "'"));
            
            return createResponse(200, body);
            
        } catch (Exception e) {
            context.getLogger().log("Error: " + e.getMessage());
            return createResponse(500, "{\"error\": \"" + e.getMessage() + "\"}");
        }
    }

    private APIGatewayProxyResponseEvent createResponse(int statusCode, String body) {
        // Hier wurden KEINE CORS Header gesetzt, weil AWS das machen sollte
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(statusCode)
                .withBody(body);
    }

    private String invokeLambda(String functionName, String payload) {
        try {
            InvokeRequest request = InvokeRequest.builder()
                    .functionName(functionName)
                    .payload(SdkBytes.fromUtf8String(payload))
                    .build();
            var response = lambdaClient.invoke(request);
            return new String(response.payload().asByteArray());
        } catch (Exception e) {
            return "Error calling " + functionName + ": " + e.getMessage();
        }
    }
}