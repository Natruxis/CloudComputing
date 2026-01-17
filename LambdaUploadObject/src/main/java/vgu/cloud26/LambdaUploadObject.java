package vgu.cloud26;

import java.util.Base64;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

// WICHTIG: Hier steht jetzt Map<String, Object> statt APIGatewayProxyRequestEvent!
public class LambdaUploadObject implements RequestHandler<Map<String, Object>, String> {

    @Override
    public String handleRequest(Map<String, Object> input, Context context) {
        
        context.getLogger().log("Upload Lambda gestartet via Orchestrator.");

        // Wir lesen die Daten direkt aus der Map, die der Orchestrator schickt
        String content = (String) input.get("content");
        String objName = (String) input.get("key");
        String bucketName = (String) input.get("bucket");
        
        if (content == null || objName == null || bucketName == null) {
            context.getLogger().log("FEHLER: Parameter fehlen!");
            return "Fehler: Parameter fehlen";
        }

        context.getLogger().log("Versuche Upload: " + objName + " nach " + bucketName);

        try {
            byte[] objBytes = Base64.getDecoder().decode(content.getBytes());
            
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objName)
                    .build();

            S3Client s3Client = S3Client.builder()
                    .region(Region.AP_SOUTHEAST_2)
                    .build();
                    
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(objBytes));
            
            context.getLogger().log("Upload erfolgreich!");
            return "Upload erfolgreich";
            
        } catch (Exception e) {
            context.getLogger().log("CRASH beim S3 Upload: " + e.getMessage());
            // Wir werfen den Fehler weiter, damit der Orchestrator (hoffentlich) merkt, dass was faul ist
            throw new RuntimeException(e);
        }
    }
}