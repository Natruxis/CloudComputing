package vgu.cloud26;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;

public class LambdaResizeOrchestrator implements RequestHandler<Map<String, Object>, APIGatewayProxyResponseEvent> {

    private final LambdaClient lambdaClient = LambdaClient.builder()
            .region(Region.AP_SOUTHEAST_2)
            .build();

    @Override
    public APIGatewayProxyResponseEvent handleRequest(Map<String, Object> input, Context context) {
        APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
        
        try {
            context.getLogger().log("LambdaResizeOrchestrator gestartet");
            
            // 1. Body aus dem Input extrahieren
            String body = extractBodyFromInput(input, context);
            if (body == null || body.isEmpty()) {
                context.getLogger().log("Fehler: Leerer oder null Body");
                return response
                        .withStatusCode(400)
                        .withBody("{\"error\": \"Request body is missing or empty\"}");
            }
            
            context.getLogger().log("Body empfangen: " + body.substring(0, Math.min(body.length(), 100)) + "...");
            
            // 2. JSON parsen
            JSONObject inputJson = new JSONObject(body);
            String content = inputJson.getString("content");
            String key = inputJson.getString("key");
            
            context.getLogger().log("Verarbeite Datei: " + key);
            
            // 3. Prüfen ob es sich um ein Bild handelt
            if (!isImageFile(key)) {
                context.getLogger().log("Datei ist kein Bild: " + key);
                return response
                        .withStatusCode(400)
                        .withBody("{\"error\": \"Only image files can be processed\"}");
            }
            
            // 4. SCHRITT 1: Original in lmitu16 hochladen
            JSONObject uploadPayload = new JSONObject();
            uploadPayload.put("content", content);
            uploadPayload.put("key", key);
            uploadPayload.put("bucket", "lmitu16");
            
            context.getLogger().log("Rufe LambdaUploadObject auf...");
            String uploadResult = invokeLambdaSync("LambdaUploadObject", uploadPayload.toString(), context);
            context.getLogger().log("Upload Ergebnis: " + uploadResult);
            
            if (uploadResult.contains("Fehler") || uploadResult.contains("Exception")) {
                throw new RuntimeException("Upload fehlgeschlagen: " + uploadResult);
            }
            
            // 5. SCHRITT 2: Bild resizen und in myresizedimagesbucket speichern
            JSONObject resizePayload = new JSONObject();
            resizePayload.put("srcBucket", "lmitu16");
            resizePayload.put("srcKey", key);
            resizePayload.put("dstBucket", "myresizedimagesbucket");
            resizePayload.put("dstKey", "resized-" + key);
            
            context.getLogger().log("Rufe LambdaResizer auf...");
            String resizeResult = invokeLambdaSync("LambdaResizer", resizePayload.toString(), context);
            context.getLogger().log("Resize Ergebnis: " + resizeResult);
            
            if (resizeResult.contains("Fehler") || resizeResult.contains("Exception")) {
                throw new RuntimeException("Resize fehlgeschlagen: " + resizeResult);
            }
            
            context.getLogger().log("Bildverarbeitung erfolgreich abgeschlossen");
            
            // 6. Erfolgreiche Response zurückgeben
            JSONObject successResponse = new JSONObject();
            successResponse.put("message", "Bild erfolgreich verarbeitet");
            successResponse.put("originalKey", key);
            successResponse.put("thumbnailKey", "resized-" + key);
            successResponse.put("originalBucket", "lmitu16");
            successResponse.put("thumbnailBucket", "myresizedimagesbucket");
            
            return response
                    .withStatusCode(200)
                    .withBody(successResponse.toString())
                    .withHeaders(Map.of(
                        "Content-Type", "application/json",
                        "Access-Control-Allow-Origin", "*",
                        "Access-Control-Allow-Headers", "*",
                        "Access-Control-Allow-Methods", "POST, OPTIONS"
                    ));
                    
        } catch (Exception e) {
            context.getLogger().log("Fehler in LambdaResizeOrchestrator: " + e.getMessage());
            e.printStackTrace();
            
            JSONObject errorResponse = new JSONObject();
            errorResponse.put("error", e.getMessage());
            errorResponse.put("type", e.getClass().getSimpleName());
            
            return response
                    .withStatusCode(500)
                    .withBody(errorResponse.toString())
                    .withHeaders(Map.of(
                        "Content-Type", "application/json",
                        "Access-Control-Allow-Origin", "*"
                    ));
        }
    }

    private String extractBodyFromInput(Map<String, Object> input, Context context) {
        try {
            // Wenn der Body direkt als String vorhanden ist
            if (input.containsKey("body")) {
                String body = (String) input.get("body");
                
                // Prüfen ob der Body base64 kodiert ist
                Boolean isBase64Encoded = (Boolean) input.get("isBase64Encoded");
                if (isBase64Encoded != null && isBase64Encoded) {
                    context.getLogger().log("Body ist base64 kodiert, decodiere...");
                    return new String(Base64.getDecoder().decode(body), StandardCharsets.UTF_8);
                }
                return body;
            }
            
            // Alternative: Der gesamte Input könnte der Body sein (direktes Lambda Invoke)
            // Serialisiere den Input zu JSON
            return new JSONObject(input).toString();
            
        } catch (Exception e) {
            context.getLogger().log("Fehler beim Extrahieren des Body: " + e.getMessage());
            return null;
        }
    }

    private String invokeLambdaSync(String functionName, String payload, Context context) {
        try {
            context.getLogger().log("Invoke Lambda: " + functionName);
            
            InvokeRequest request = InvokeRequest.builder()
                    .functionName(functionName)
                    .payload(SdkBytes.fromUtf8String(payload))
                    .build();
                    
            InvokeResponse invokeResponse = lambdaClient.invoke(request);
            
            // Response auslesen
            ByteBuffer responsePayload = invokeResponse.payload().asByteBuffer();
            String responseString = StandardCharsets.UTF_8.decode(responsePayload).toString();
            
            // Wenn die aufgerufene Lambda einen Fehler geworfen hat
            if (invokeResponse.statusCode() != 200) {
                context.getLogger().log("Lambda " + functionName + " returned status: " + invokeResponse.statusCode());
                try {
                    JSONObject errorObj = new JSONObject(responseString);
                    return "Fehler: " + errorObj.optString("errorMessage", errorObj.toString());
                } catch (Exception e) {
                    return "Fehler: " + responseString;
                }
            }
            
            return responseString;
            
        } catch (Exception e) {
            context.getLogger().log("Fehler beim Aufruf von " + functionName + ": " + e.getMessage());
            return "Exception: " + e.getMessage();
        }
    }

    private boolean isImageFile(String fileName) {
        if (fileName == null) return false;
        String lowerCaseName = fileName.toLowerCase();
        return lowerCaseName.endsWith(".jpg") || 
               lowerCaseName.endsWith(".jpeg") || 
               lowerCaseName.endsWith(".png") || 
               lowerCaseName.endsWith(".gif") || 
               lowerCaseName.endsWith(".bmp") || 
               lowerCaseName.endsWith(".webp");
    }
}