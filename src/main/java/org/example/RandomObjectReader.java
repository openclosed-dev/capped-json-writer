package org.example;

import java.util.Collections;
import java.util.Random;

import org.springframework.batch.item.ItemReader;

import jakarta.json.Json;
import jakarta.json.JsonBuilderFactory;
import jakarta.json.JsonObject;

public class RandomObjectReader implements ItemReader<JsonObject> {

    private final int maxObjectCount;

    private final Random random = new Random();
    private final JsonBuilderFactory jsonBuilderFactory;

    private int objectCount;

    public RandomObjectReader(int maxObjectCount) {
        this.maxObjectCount = maxObjectCount;
        this.jsonBuilderFactory = Json.createBuilderFactory(Collections.emptyMap());
        this.objectCount = 0;
    }

    @Override
    public JsonObject read() {
        if (objectCount >= maxObjectCount) {
            return null;
        }
        return generateJsonObject(String.valueOf(++objectCount));
    }

    /**
     * Generates a JSON object randomly populated.
     * @param id the identifier of the object
     * @return generated JSON object
     */
    private JsonObject generateJsonObject(String id) {
       var builder = jsonBuilderFactory.createObjectBuilder();

       builder.add("id", id);

       int numberOfKeys = random.nextInt(50);
       for (int i = 0; i < numberOfKeys; i++) {
           builder.add("key" + i, generateRandomString());
       }

       return builder.build();
   }

    private String generateRandomString() {
        int length = random.nextInt(128);
        return random.ints('a', 'z')
            .limit(length)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }
}
