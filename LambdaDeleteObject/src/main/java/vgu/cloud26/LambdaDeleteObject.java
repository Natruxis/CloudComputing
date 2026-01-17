package vgu.cloud26;

import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class LambdaDeleteObject implements RequestHandler<Map<String, Object>, String> {

    @Override
    public String handleRequest(Map<String, Object> input, Context context) {
        context.getLogger().log("Delete Object Lambda gestartet");
        
        try {
            // Extrahiere Parameter
            String bucket = (String) input.get("bucket");
            String key = (String) input.get("key");
            
            if (bucket == null || key == null) {
                return "{\"error\": \"Bucket oder Key fehlt\"}";
            }
            
            context.getLogger().log("Lösche: " + bucket + "/" + key);
            
            // S3 Client erstellen
            S3Client s3Client = S3Client.builder()
                    .region(Region.AP_SOUTHEAST_2)
                    .build();
            
            // Delete Request erstellen
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            
            // Löschen durchführen
            s3Client.deleteObject(deleteRequest);
            
            context.getLogger().log("Erfolgreich gelöscht: " + bucket + "/" + key);
            
            return "{\"success\": true, \"message\": \"'" + key + "' aus Bucket '" + bucket + "' gelöscht\"}";
            
        } catch (S3Exception e) {
            context.getLogger().log("S3 Fehler: " + e.getMessage());
            // Wenn Datei nicht existiert, ist das auch okay (Idempotenz)
            if (e.statusCode() == 404) {
                return "{\"success\": true, \"message\": \"Datei existierte nicht (bereits gelöscht)\"}";
            }
            return "{\"error\": \"S3 Fehler: " + e.getMessage() + "\"}";
        } catch (Exception e) {
            context.getLogger().log("Allgemeiner Fehler: " + e.getMessage());
            return "{\"error\": \"Löschen fehlgeschlagen: " + e.getMessage() + "\"}";
        }
    }
}