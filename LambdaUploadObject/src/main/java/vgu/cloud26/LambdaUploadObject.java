package vgu.cloud26;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsUtilities;
import software.amazon.awssdk.services.rds.model.GenerateAuthenticationTokenRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class LambdaUploadObject implements RequestHandler<Map<String, Object>, String> {

    // --- DATENBANK KONFIGURATION (Identisch zu LambdaGetPhotosDB) ---
    private static final String RDS_INSTANCE_HOSTNAME = "database-1.cdq0ekg8q844.ap-southeast-2.rds.amazonaws.com";
    private static final int RDS_INSTANCE_PORT = 3306;
    private static final String DB_USER = "cloud26";
    private static final String JDBC_URL = "jdbc:mysql://" + RDS_INSTANCE_HOSTNAME + ":" + RDS_INSTANCE_PORT + "/cloud26";

    @Override
    public String handleRequest(Map<String, Object> input, Context context) {
        
        context.getLogger().log("Upload Lambda gestartet via Orchestrator.");

        // 1. Daten auslesen (Jetzt auch Email und Description!)
        String content = (String) input.get("content");
        String objName = (String) input.get("key");
        String bucketName = (String) input.get("bucket");
        String email = (String) input.get("email");         // NEU
        String description = (String) input.get("description"); // NEU
        
        // Fallback, falls null (damit die DB nicht abstürzt)
        if (email == null) email = "";
        if (description == null) description = "";

        if (content == null || objName == null || bucketName == null) {
            context.getLogger().log("FEHLER: Wichtige Parameter (content/key/bucket) fehlen!");
            return "Fehler: Parameter fehlen";
        }

        context.getLogger().log("Versuche Upload: " + objName + " nach " + bucketName);

        try {
            // --- SCHRITT A: S3 UPLOAD ---
            byte[] objBytes = Base64.getDecoder().decode(content.getBytes());
            
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objName)
                    .build();

            S3Client s3Client = S3Client.builder()
                    .region(Region.AP_SOUTHEAST_2)
                    .build();
                    
            s3Client.putObject(putObjectRequest, RequestBody.fromBytes(objBytes));
            context.getLogger().log("S3 Upload erfolgreich!");

            // --- SCHRITT B: DATENBANK EINTRAG (INSERT) ---
            context.getLogger().log("Verbinde zur Datenbank zum Speichern der Metadaten...");
            
            Class.forName("com.mysql.cj.jdbc.Driver");
            
            try (Connection connection = DriverManager.getConnection(JDBC_URL, setMySqlConnectionProperties())) {
                
                // Der SQL Befehl zum Einfügen
                String sql = "INSERT INTO Photos (Description, S3Key, Email) VALUES (?, ?, ?)";
                
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, description);
                    statement.setString(2, objName);     // S3Key ist der Dateiname
                    statement.setString(3, email);
                    
                    statement.executeUpdate();
                    context.getLogger().log("Datenbank Eintrag erfolgreich gespeichert!");
                }
            }

            return "Upload und Speichern erfolgreich";
            
        } catch (Exception e) {
            context.getLogger().log("CRASH: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    // --- HILFSMETHODEN FÜR DB AUTH (Identisch zu LambdaGetPhotosDB) ---
    private static Properties setMySqlConnectionProperties() throws Exception {
        Properties mysqlConnectionProperties = new Properties();
        mysqlConnectionProperties.setProperty("useSSL", "true");
        mysqlConnectionProperties.setProperty("user", DB_USER);
        mysqlConnectionProperties.setProperty("password", generateAuthToken());
        return mysqlConnectionProperties;
    }

    private static String generateAuthToken() throws Exception {
        RdsUtilities rdsUtilities = RdsUtilities.builder().build();
        return rdsUtilities.generateAuthenticationToken(
                GenerateAuthenticationTokenRequest.builder()
                        .hostname(RDS_INSTANCE_HOSTNAME)
                        .port(RDS_INSTANCE_PORT)
                        .username(DB_USER)
                        .region(Region.AP_SOUTHEAST_2)
                        .credentialsProvider(DefaultCredentialsProvider.create())
                        .build());
    }
}