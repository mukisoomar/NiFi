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
package com.muki.nifi.customprocessors.custom_hl7;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"HL7_Custom"})
@CapabilityDescription("Parse an HL7 Message Segment and transform to JSON")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "SEG_NAME",
        description = "SEG_NAME is the Message Segment to be parsed from the HL7 Message")})
@WritesAttributes({@WritesAttribute(attribute = "ParsedMessage",
        description = "Parses the message segment and writes the json formated string with name value pairs")})
public class ExtractSegmentAttributes extends AbstractProcessor {

    public static final PropertyDescriptor SEG_NAME = new PropertyDescriptor
            .Builder().name("SEG_NAME")
            .displayName("HL7 Message Segment Name")
            .description("The Message Segment Name that needs to be parsed into attributes")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Successfully parsed attributes from the HL7 Message segment")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failed to parse the attributes from the HL7 Message segment")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SEG_NAME);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        String parsedResult=null;

        // Do nothing if no flowfile
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Get the flowfile content
        final byte[] buffer = new byte [(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, buffer);
            }
        });

        // Read the HL7 Message into a String
        final String hl7Message = new String(buffer);

        getLogger().info("hl7Message input was:" + hl7Message);

        String messageSegmentName = context.getProperty("SEG_NAME").getValue();
        getLogger().info("Message Segment Name input was:" + messageSegmentName);

        try {
            final Map<String,String> attributes = parseMessageSegment(hl7Message, messageSegmentName);

            getLogger().info("parsed result :" + attributes.toString());
            flowFile = session.putAllAttributes(flowFile,attributes);
            getLogger().info("Added the following attributes for {}: {} ", new Object[]{flowFile, attributes});


        } catch (Exception e) {
            getLogger().error("Failed to extract attributes from {} due to {}: ", new Object[]{flowFile, e});
            session.transfer(flowFile, FAILURE);
        }

        session.transfer(flowFile, SUCCESS);
    }

    public static Map<String, String> parseMessageSegment(String HL7Message, String messageSegmentName) throws IOException {

        Map<String, String> docMap = new HashMap<String, String>();
        final String SEPARATOR="|";

        // Readline from the message. Parse the line using the delimiter as the token
        // If the first value - which is the segment name happens to be what is passed, add it to the arraylist.

        BufferedReader reader = new BufferedReader(new StringReader(HL7Message));
        String line;
        line = reader.readLine();

        while (line != null) {

            if (line.startsWith(messageSegmentName)) {
                System.out.println("Printing line with message segment name: " + line);

                StringTokenizer st = new StringTokenizer(line, SEPARATOR);

                int docPosition = 0;
                for (; st.hasMoreTokens(); ) {
                    String docToken = st.nextToken().trim();
                    docPosition++;
                    docMap.put(""+docPosition, docToken);
                }
                break;
            }
            line = reader.readLine();
        }
        return docMap;
    }
}