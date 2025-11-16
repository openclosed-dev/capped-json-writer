package org.example;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.stream.JsonGeneratorFactory;

public class CappedJsonWriter implements ItemWriter<JsonObject> {

    private static final Logger LOG = LoggerFactory.getLogger(CappedJsonWriter.class);

    private final Charset charset;

    private final byte[] openingBytes;
    private final byte[] closingBytes;

    private final int maxJsonSizeInBytes;
    private final int maxObjectSizeInBytes;

    private final ByteArrayOutputStream jsonBuffer;
    // buffer to hold single JSON object
    private final ByteArrayOutputStream singleBuffer;

    private final JsonGeneratorFactory generatorFactory;

    private int objectCount;
    private int fileCount;

    public CappedJsonWriter(int maxJsonSizeInBytes, Charset charset) {
        this.charset = charset;

        this.openingBytes = "{\"values\":[".getBytes(charset);
        this.closingBytes = "]}".getBytes(charset);

        this.maxJsonSizeInBytes = maxJsonSizeInBytes;
        this.maxObjectSizeInBytes = maxJsonSizeInBytes - this.openingBytes.length - this.closingBytes.length;

        this.jsonBuffer = new ByteArrayOutputStream();
        this.singleBuffer = new ByteArrayOutputStream();
        this.generatorFactory = Json.createGeneratorFactory(Collections.emptyMap());
    }

    @Override
    public void write(Chunk<? extends JsonObject> chunk) throws Exception {
        for (var object : chunk) {
            writeJsonObject(object);
        }
        flush();
    }

    private void writeJsonObject(JsonObject object) {

        final var serialized = serializeJsonObject(object);
        final var objectSize = serialized.length;

        if (objectSize > maxObjectSizeInBytes) {
            throw new IllegalArgumentException("Too large object: " + objectSize + " bytes");
        }

        this.objectCount++;

        if (!hasEnoughSpace(objectSize)) {
            flushBuffer();
        }

        final int currentSize = addJsonObjectToBuffer(serialized);

        LOG.info("Total JSON objects: {}", objectCount);
        LOG.info("Current buffer size: {} bytes", currentSize);
    }

    private void flush() {
        if (jsonBuffer.size() > 0) {
            flushBuffer();
        }
    }

    private byte[] serializeJsonObject(JsonObject object) {
        this.singleBuffer.reset();
        try (var g = this.generatorFactory.createGenerator(this.singleBuffer, this.charset)) {
            g.write(object);
        }
        return this.singleBuffer.toByteArray();
    }

    private int addJsonObjectToBuffer(byte[] serialized) {
        if (jsonBuffer.size() == 0) {
            jsonBuffer.writeBytes(this.openingBytes);
        } else {
            jsonBuffer.write(',');
        }
        jsonBuffer.writeBytes(serialized);
        return jsonBuffer.size();
    }

    private boolean hasEnoughSpace(int objectSize) {
        final int currentSize = jsonBuffer.size();

        int requiredSize = objectSize + this.closingBytes.length;
        if (currentSize == 0) {
            requiredSize += this.openingBytes.length;
        } else {
            requiredSize += 1;
        }

        return currentSize + requiredSize <= this.maxJsonSizeInBytes;
    }

    private void flushBuffer() {
        jsonBuffer.writeBytes(closingBytes);

        var bytes = jsonBuffer.toByteArray();
        LOG.info("Flushing JSON file: {} bytes", bytes.length);

        processFinishedJson(bytes);

        jsonBuffer.reset();
    }

    private void processFinishedJson(byte[] bytes) {

        var dir = Path.of("build");
        var path = dir.resolve(++fileCount + ".json");

        try {
            Files.createDirectories(dir);
            LOG.info("Saving finished JSON file in '{}'", path);
            try (var os = Files.newOutputStream(path)) {
                os.write(bytes);
            }
            LOG.info("Saved finished JSON file in '{}'", path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
