# useful
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.commons.io.IOUtils;

import scala.util.Try;

public class S3StreamReader {
    private final AmazonS3 s3Client;
    private final long bufferSize;

    public S3StreamReader(AmazonS3 s3Client, long bufferSize) {
        this.s3Client = s3Client;
        this.bufferSize = bufferSize;
    }

    public Try<InputStream> get(String bucketName, String key) {
        return Try.of(() -> {
            long totalSize = getSize(bucketName, key);
            Enumeration<S3ObjectInputStream> s3Enumeration = getEnumeration(bucketName, key, totalSize);
            Enumeration<ByteArrayInputStream> bufferedEnumeration = getBufferedEnumeration(s3Enumeration);
            return new SequenceInputStream(bufferedEnumeration);
        });
    }

    private long getSize(String bucketName, String key) {
        return s3Client.getObjectMetadata(bucketName, key).getContentLength();
    }

    private Enumeration<S3ObjectInputStream> getEnumeration(String bucketName, String key, long totalSize) {
        return new Enumeration<S3ObjectInputStream>() {
            long currentPosition = 0L;

            @Override
            public boolean hasMoreElements() {
                return currentPosition < totalSize;
            }

            @Override
            public S3ObjectInputStream nextElement() {
                if (!hasMoreElements()) {
                    throw new NoSuchElementException();
                }
                GetObjectRequest getRequest = new GetObjectRequest(bucketName, key)
                        .withRange(currentPosition, currentPosition + bufferSize - 1);
                currentPosition += bufferSize;
                return s3Client.getObject(getRequest).getObjectContent();
            }
        };
    }

    private Enumeration<ByteArrayInputStream> getBufferedEnumeration(Enumeration<S3ObjectInputStream> underlying) {
        return new Enumeration<ByteArrayInputStream>() {
            @Override
            public boolean hasMoreElements() {
                return underlying.hasMoreElements();
            }

            @Override
            public ByteArrayInputStream nextElement() {
                if (!hasMoreElements()) {
                    throw new NoSuchElementException();
                }
                S3ObjectInputStream nextStream = underlying.nextElement();
                try {
                    byte[] byteArray = IOUtils.toByteArray(nextStream);
                    nextStream.close();
                    return new ByteArrayInputStream(byteArray);
                } catch (Exception e) {
                    throw new RuntimeException("Error reading S3 object content", e);
                }
            }
        };
    }
}
