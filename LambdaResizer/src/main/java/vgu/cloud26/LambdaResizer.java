package vgu.cloud26;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Map;

import javax.imageio.ImageIO;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

// Änderung: Wir akzeptieren direkt eine Map (genau wie beim Upload)
public class LambdaResizer implements RequestHandler<Map<String, Object>, String> {

    private static final float MAX_DIMENSION = 300; // Etwas größer als 100, damit man was erkennt
    private final String JPG_TYPE = "jpg";
    private final String JPG_MIME = "image/jpeg";
    private final String PNG_TYPE = "png";
    private final String PNG_MIME = "image/png";

    private final S3Client s3Client = S3Client.builder()
            .region(Region.AP_SOUTHEAST_2)
            .build();

    @Override
    public String handleRequest(Map<String, Object> input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("RESIZER GESTARTET via Orchestrator");

        try {
            // 1. Parameter auslesen
            String srcBucket = (String) input.get("srcBucket");
            String srcKey = (String) input.get("srcKey");
            String dstBucket = (String) input.get("dstBucket");
            String dstKey = (String) input.get("dstKey");

            logger.log("Download von: " + srcBucket + "/" + srcKey);

            // 2. Download
            InputStream s3ObjectStream = getObject(srcBucket, srcKey);
            BufferedImage srcImage = ImageIO.read(s3ObjectStream);

            if (srcImage == null) {
                logger.log("FEHLER: Bild konnte nicht gelesen werden (null).");
                return "Fehler: Kein Bild";
            }

            // 3. Resize
            BufferedImage newImage = resizeImage(srcImage);

            // 4. Upload
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            // Wir speichern einfach alles als JPG, das ist am robustesten für den Test
            ImageIO.write(newImage, "jpg", outputStream);
            
            logger.log("Upload nach: " + dstBucket + "/" + dstKey);
            
            putObject(outputStream, dstBucket, dstKey, JPG_MIME);

            return "Erfolg: Resize fertig";

        } catch (Exception e) {
            // Dieser Fehler wird in den CloudWatch Logs stehen!
            logger.log("CRASH im Resizer: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Resizer fehlgeschlagen: " + e.getMessage());
        }
    }

    private InputStream getObject(String bucket, String key) {
        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucket).key(key).build());
    }

    private void putObject(ByteArrayOutputStream outputStream, String bucket, String key, String contentType) {
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType(contentType)
                .build(),
                RequestBody.fromBytes(outputStream.toByteArray()));
    }

    private BufferedImage resizeImage(BufferedImage srcImage) {
        int srcWidth = srcImage.getWidth();
        int srcHeight = srcImage.getHeight();
        float scalingFactor = Math.min(MAX_DIMENSION / srcWidth, MAX_DIMENSION / srcHeight);
        int width = (int) (scalingFactor * srcWidth);
        int height = (int) (scalingFactor * srcHeight);

        BufferedImage resizedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_RGB);
        Graphics2D graphics = resizedImage.createGraphics();
        graphics.setPaint(Color.white);
        graphics.fillRect(0, 0, width, height);
        graphics.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
        graphics.drawImage(srcImage, 0, 0, width, height, null);
        graphics.dispose();
        return resizedImage;
    }
}