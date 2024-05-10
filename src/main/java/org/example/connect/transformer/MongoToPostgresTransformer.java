package org.example.connect.transformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.example.connect.constant.Operation.*;
import static org.example.connect.constant.Field.*;
import static org.example.connect.constant.Table.*;


public class MongoToPostgresTransformer<R extends ConnectRecord<R>> implements Transformation<R> {

    private final Logger log = LoggerFactory.getLogger(MongoToPostgresTransformer.class);

    private final ObjectMapper mapper = new ObjectMapper();

    private final Map<Integer, String> tableList = new HashMap<Integer, String>() {{
        put(1, ACCESS_CONTROL_POLICY);
        put(2, AE);
        put(3, CONTAINER);
        put(4, CONTENT_INSTANCE);
        put(5, CSE_BASE);
        put(23, SUBSCRIPTION);
    }};

    private final Map<Integer, List<String>> schemaList = new HashMap<Integer, List<String>>() {{
        put(1, new ArrayList<>(Arrays.asList(RESOURCE_ID, PARENT_ID, CREATION_TIME, LAST_MODIFIED_TIME, EXPIRATION_TIME, RESOURCE_NAME, PRIVILEGES, SELF_PRIVILEGES, INTERNAL_URI, INTERNAL_SRN, INTERNAL_RLVL)));
        put(2, new ArrayList<>(Arrays.asList(RESOURCE_ID, PARENT_ID, CREATION_TIME, LAST_MODIFIED_TIME, EXPIRATION_TIME, RESOURCE_NAME, APP_ID, AE_ID, REQUEST_REACHABILITY, INTERNAL_URI, INTERNAL_SRN, INTERNAL_RLVL))); // list type : lbl, acpi, srv
        put(3, new ArrayList<>(Arrays.asList(RESOURCE_ID, PARENT_ID, CREATION_TIME, LAST_MODIFIED_TIME, EXPIRATION_TIME, RESOURCE_NAME, STATE_TAG, CREATOR, MAX_NR_OF_INSTANCES, MAX_BYTE_SIZE, MAX_INSTANCE_AGE, CURRENT_NR_OF_INSTANCES, CURRENT_BYTE_SIZE, INTERNAL_URI, INTERNAL_SRN, INTERNAL_RLVL)));
        put(4, new ArrayList<>(Arrays.asList(RESOURCE_ID, PARENT_ID, CREATION_TIME, LAST_MODIFIED_TIME, EXPIRATION_TIME, RESOURCE_NAME, STATE_TAG, CREATOR, CONTENT_INFO, CONTENT_SIZE, CONTENT, INTERNAL_URI, INTERNAL_SRN, INTERNAL_RLVL)));
        put(5, new ArrayList<>(Arrays.asList(RESOURCE_ID, PARENT_ID, CREATION_TIME, LAST_MODIFIED_TIME, EXPIRATION_TIME, RESOURCE_NAME, CSE_TYPE, CSE_ID))); // list type : srt, poa, srv
        put(23, new ArrayList<>(Arrays.asList(RESOURCE_ID, PARENT_ID, CREATION_TIME, LAST_MODIFIED_TIME, EXPIRATION_TIME, RESOURCE_NAME, CREATOR, EVENT_NOTIFICATION_CRITERIA, INTERNAL_URI, INTERNAL_SRN, INTERNAL_RLVL))); // list type : nu
    }};

    private final List<String> jsonStringFields = new ArrayList<String>() {{
        add(PRIVILEGES);
        add(SELF_PRIVILEGES);
        add(EVENT_NOTIFICATION_CRITERIA);
    }};

    private final Map<String, Schema> fieldSchemaList = new HashMap<String, Schema>() {{
        // common
        put(RESOURCE_ID, Schema.STRING_SCHEMA);
        put(PARENT_ID, Schema.OPTIONAL_STRING_SCHEMA);
        put(CREATION_TIME, Schema.STRING_SCHEMA);
        put(LAST_MODIFIED_TIME, Schema.STRING_SCHEMA);
        put(EXPIRATION_TIME, Schema.OPTIONAL_STRING_SCHEMA);
        put(RESOURCE_NAME, Schema.OPTIONAL_STRING_SCHEMA);

        // internal
        put(INTERNAL_URI, Schema.OPTIONAL_STRING_SCHEMA);
        put(INTERNAL_SRN, Schema.OPTIONAL_STRING_SCHEMA);
        put(INTERNAL_RLVL, Schema.OPTIONAL_INT32_SCHEMA);

        // acp
        put(PRIVILEGES, Schema.OPTIONAL_STRING_SCHEMA); // json string
        put(SELF_PRIVILEGES, Schema.OPTIONAL_STRING_SCHEMA); // json string

        // ae
        put(APP_ID, Schema.OPTIONAL_STRING_SCHEMA);
        put(AE_ID, Schema.OPTIONAL_STRING_SCHEMA);
        put(REQUEST_REACHABILITY, Schema.OPTIONAL_BOOLEAN_SCHEMA);

        // cnt
        put(MAX_NR_OF_INSTANCES, Schema.OPTIONAL_INT64_SCHEMA);
        put(MAX_BYTE_SIZE, Schema.OPTIONAL_INT64_SCHEMA);
        put(MAX_INSTANCE_AGE, Schema.OPTIONAL_INT64_SCHEMA);
        put(CURRENT_NR_OF_INSTANCES, Schema.OPTIONAL_INT32_SCHEMA);
        put(CURRENT_BYTE_SIZE, Schema.OPTIONAL_INT32_SCHEMA);

        // cin
        put(STATE_TAG, Schema.OPTIONAL_INT32_SCHEMA);
        put(CREATOR, Schema.OPTIONAL_STRING_SCHEMA);
        put(CONTENT_INFO, Schema.OPTIONAL_STRING_SCHEMA);
        put(CONTENT_SIZE, Schema.OPTIONAL_INT32_SCHEMA);
        put(CONTENT, Schema.OPTIONAL_STRING_SCHEMA);

        // sub
        put(EVENT_NOTIFICATION_CRITERIA, Schema.OPTIONAL_STRING_SCHEMA); // json string
    }};

    @Override
    public R apply(R record) {
        //log.info("Record : " + record.toString());
        //log.info("Record key schema : " + record.keySchema().toString());
        //log.info("Record key : " + record.key().toString());
        //log.info("Record value schema : " + record.valueSchema().toString());
        //log.info("Record value : " + record.value().toString());

        if (record.valueSchema() == null) {
            // Skip non-structured data
            return record;
        }

        //log.info("Record value : " + record.value().toString());

        try {
            Map<String, Object> valueMap = mapper.readValue(record.value().toString(), Map.class);

            //log.info("Record value map : " + valueMap.keySet());
            switch (valueMap.get("operationType").toString().toLowerCase()) {
                case INSERT:
                    if (valueMap.containsKey("fullDocument")) {
                        Map<String, Object> mongoDoc = (Map<String, Object>) valueMap.get("fullDocument");
                        //log.info("FullDocument map : " + mongoDoc.toString());

                        int type = Integer.parseInt(mongoDoc.get(RESOURCE_TYPE).toString());

                        //log.info("Data type : " + type);

                        List<String> schema = schemaList.get(type);

                        SchemaBuilder sb = new SchemaBuilder(Schema.Type.STRUCT);
                        for (String field: schema) sb.field(field, fieldSchemaList.get(field));
                        Schema valueSchema = sb.build();
                        //log.info("Record value schema fields : " + valueSchema.fields().toString());

                        Struct transformedStruct = new Struct(valueSchema);

                        if (!schema.isEmpty()) {
                            for (String key : mongoDoc.keySet()) {
                                if (schema.contains(key) && !key.equals(RESOURCE_ID)) {
                                    Object docValue = mongoDoc.get(key);
                                    if(jsonStringFields.contains(key)) docValue = docValue.toString();
                                    transformedStruct.put(key, docValue);
                                }
                            }
                        }
                        //log.info("Transformed : " + transformedStruct.toString());

                        //log.info("new record : " + newRecord.toString());

                        R newRecord = record.newRecord(
                                tableList.get(type),
                                record.kafkaPartition(),
                                record.keySchema(),
                                mongoDoc.get(RESOURCE_ID),
                                valueSchema,
                                transformedStruct,
                                record.timestamp() );

/*                        ConnectRecord[] subRecords = {record.newRecord(
                                tableList.get(type),
                                record.kafkaPartition(),
                                record.keySchema(),
                                mongoDoc.get(RESOURCE_ID),
                                valueSchema,
                                transformedStruct,
                                record.timestamp() )};*/

                        return newRecord;
                    } else {
                        log.info("ValueMap is empty");
                    }
                    break;
                case UPDATE:
                    break;
            }

        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

}