
package org.example;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.serializer.FhirResourceSerializer;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import netscape.javascript.JSObject;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import com.fasterxml.jackson.databind.JsonNode;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;

public class Main {

    final static String INPUT_TOPIC = "wearables-raw";
    final static String OUTPUT_TOPIC = "wearables-fhri";
    final static String KAFKA_BROKER_ENV_VAR = "KAFKA_BROKER";
    final static String APP_ID = "fhri-mapper";

    public static void main(String[] args) throws InterruptedException {

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> lowerCaseStrings = builder.stream(INPUT_TOPIC);
        KStream<String, String> upperCaseStrings = lowerCaseStrings.mapValues(new ValueMapper<String, String>() {
            @Override
            public String apply(String str) {
                SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_7, OptionPreset.PLAIN_JSON);
                SchemaGeneratorConfig config = configBuilder.build();
                SchemaGenerator generator = new SchemaGenerator(config);
                JsonNode jsonSchema = generator.generateSchema(Observation.class);

                System.out.println(jsonSchema.toPrettyString());
                System.out.println("converting now" + str);

                FhirContext ctx = FhirContext.forR4();

                Gson gson = new Gson();

                ExampleInput parsedInput = new ExampleInput();

                try {
                    parsedInput = gson.fromJson(str, ExampleInput.class);
                }catch(JsonSyntaxException jse){
                    System.out.println("Could not parse string to json, error: "+ jse.getMessage());
                    return "";
                }

                //here we would have to add the actual logic of parsing the different kinds of health data
                //Documentation[https://hl7.org/fhir/observation.html]

                //we create a resource for the observation (I assume since we are working with wearables, we will mostly be using observations)
                Observation obs = new Observation();

                //we set the type of observation (there are codes and strings to identify them AFAIK)
                obs.setId("heart-rate");

                //set other properties of the observation (like time, status, result)
                obs.setStatus(Observation.ObservationStatus.FINAL);

                DateTimeType dtt = new DateTimeType(new Date(parsedInput.timestamp));
                obs.setEffective(dtt);

                //drastically simplified compared to the way it's actually supposed to be done
                obs.setValue(new IntegerType(parsedInput.heartrate));

                String fhirJson = ctx.newJsonParser().encodeResourceToString(obs);

                System.out.println(fhirJson);

                return fhirJson;
            }
        });
        upperCaseStrings.to(OUTPUT_TOPIC);

        Topology topology = builder.build();
        KafkaStreams streamsApp = new KafkaStreams(topology, getKafkaStreamsConfig());
        streamsApp.start();
        System.out.println("Started streams app.....");

        Runtime.getRuntime().addShutdownHook(new Thread(streamsApp::close));
        new CountDownLatch(1).await();
    }

    static Properties getKafkaStreamsConfig() {

        // Read broker info from environment variable KAFKA_BROKER
        String envValue = System.getenv(KAFKA_BROKER_ENV_VAR);

        String bootstrapServers;
        if (envValue == null || envValue.trim().isEmpty()) {
            // Fallback to sensible default
            bootstrapServers = "localhost:9092";
            System.out.println("Env var '" + KAFKA_BROKER_ENV_VAR + "' not set. Using default bootstrap servers: " + bootstrapServers);
        } else {
            String trimmed = envValue.trim();
            // If the value already looks like a full bootstrap servers string (contains ':' or commas), use as-is
            if (trimmed.contains(":") || trimmed.contains(",")) {
                bootstrapServers = trimmed;
            } else {
                // Treat it as a host and append the default Kafka port
                bootstrapServers = trimmed + ":9092";
            }
            System.out.println("Using bootstrap servers from env '" + KAFKA_BROKER_ENV_VAR + "': " + bootstrapServers);
        }

        Properties configurations = new Properties();

        configurations.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configurations.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        configurations.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.().getClass().getName());
        configurations.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        configurations.put(StreamsConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000");
        configurations.put(StreamsConfig.RETRY_BACKOFF_MS_CONFIG, "500");

        return configurations;
    }

}

// One example Input for one kind of observation
class ExampleInput {
    long timestamp;
    int heartrate;
}
