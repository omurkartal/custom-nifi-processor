/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.omur.nifi.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.omur.nifi.model.SampleDataModel;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"omur", "custom", "json-builder"})
@CapabilityDescription("Get data from the input flow, add property values and create a JSON message as an output.")
//@SeeAlso({})
//@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
//@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
@DynamicProperty(name = "Generated FlowFile attribute name", value = "Generated FlowFile attribute value",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
        description = "Specifies an attribute on generated FlowFiles defined by the Dynamic Property's key and value." +
                " If Expression Language is used, evaluation will be performed only once per batch of generated FlowFiles.")
public class MyProcessor extends AbstractProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyProcessor.class);
    private static final String PLAIN_TEXT = "PLAIN_TEXT";
    private static final String JSON = "JSON";

    public static final PropertyDescriptor PROPERTY_IN_TEXT = new PropertyDescriptor
            .Builder().name("PROPERTY_IN_TEXT")
            .displayName("Input text")
            .description("Input text will be written to out flow file.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROPERTY_OUTPUT_FORMAT = new PropertyDescriptor
            .Builder().name("PROPERTY_OUTPUT_FORMAT")
            .displayName("Output format")
            .description("Output format of the processor.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(new HashSet<>(Arrays.asList(PLAIN_TEXT, JSON)))
            .defaultValue(PLAIN_TEXT)
            .build();

    public static final Relationship RELATION_SUCCESS = new Relationship.Builder()
            .name("RELATION_SUCCESS")
            .description("Relation for successful operations")
            .build();

    public static final Relationship RELATION_FAIL = new Relationship.Builder()
            .name("RELATION_FAIL")
            .description("Relation for failed operations")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROPERTY_IN_TEXT);
        descriptors.add(PROPERTY_OUTPUT_FORMAT);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(RELATION_SUCCESS);
        relationships.add(RELATION_FAIL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .dynamic(true)
                .build();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final AtomicReference<String> inputValue = new AtomicReference<>();
        String valueOfInText = context.getProperty(PROPERTY_IN_TEXT).getValue();
        String valueOfOutputFormat = context.getProperty(PROPERTY_OUTPUT_FORMAT).getValue();

        LOGGER.info("valueOfInText      :" + valueOfInText);
        LOGGER.info("valueOfOutputFormat:" + valueOfOutputFormat);

        Map<String, String> generatedAttributes = new HashMap<>();
        Map<PropertyDescriptor, String> propertyList = context.getProperties();
        for (final Map.Entry<PropertyDescriptor, String> entry : propertyList.entrySet()) {
            PropertyDescriptor property = entry.getKey();
            if (property.isDynamic() && property.isExpressionLanguageSupported()) {
                String dynamicValue = context.getProperty(property).evaluateAttributeExpressions().getValue();
                generatedAttributes.put(property.getName(), dynamicValue);
                LOGGER.info("dynamic property-> key:" + property.toString() + ", value:" + dynamicValue);
            }
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            throw new ProcessException("input flow file cannot be null!");
        }

        LOGGER.info("flow id is " + flowFile.getId());
        session.read(flowFile, inputStream -> {
            String inputText = IOUtils.toString(inputStream);
            LOGGER.info("inputText:" + inputText);
            inputValue.set(inputText);
        });

        // flow files won't be used any more should be removed. otherwise, several errors may occur in the next code bloks.
        session.remove(flowFile);

        boolean isOutFlowfileCreated = true;
        FlowFile outFlowFile = null;
        try {
            SampleDataModel sampleData = new SampleDataModel();
            sampleData.setTimestamp(LocalDateTime.now());
            sampleData.setMessageText(valueOfInText);
            sampleData.setParameterList(generatedAttributes);

            final AtomicReference<String> outMessage = new AtomicReference<>();
            if (valueOfOutputFormat.equalsIgnoreCase(JSON)) {
                ObjectMapper jsonObjectMapper = getJsonMapper();
                outMessage.set(jsonObjectMapper.writeValueAsString(sampleData));
            } else {
                outMessage.set(sampleData.toString());
            }

            outFlowFile = session.create();
            outFlowFile = session.putAttribute(outFlowFile, "input-text", inputValue.get());
            outFlowFile = session.write(outFlowFile, outputStream -> outputStream.write(outMessage.get().getBytes()));
        } catch (Exception ex) {
            isOutFlowfileCreated = false;
            ex.printStackTrace();
        }

        if (isOutFlowfileCreated) {
            session.transfer(outFlowFile, RELATION_SUCCESS);
        } else {
            session.transfer(outFlowFile, RELATION_SUCCESS);
        }
        session.commit();
    }

    private ObjectMapper getJsonMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
