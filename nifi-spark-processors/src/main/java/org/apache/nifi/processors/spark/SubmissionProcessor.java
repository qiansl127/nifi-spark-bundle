/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.spark;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.sun.corba.se.impl.orbutil.closure.Future;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.*;

/**
 * This processor will execute only once after it is started.
 * 
 * 
 */
@Tags({ "spark", "context" })
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
@TriggerWhenEmpty
@TriggerSerially
public class SubmissionProcessor extends AbstractProcessor {
    private Boolean isRunnable = true;
    private ProcessorLog LOG;
    Process sparkSubmit = null;
//    private final String COMMA_SPLIT = ",";
//    private final String EQUAL_SPLIT = "=";

    public static final PropertyDescriptor SPARK_HOME = new PropertyDescriptor.Builder()
            .name("spark home")
            .description("The spark execution home folder")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAIN_CLASS = new PropertyDescriptor.Builder()
            .name("main class")
            .description("The main class name for the applications")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SPARK_MASTER = new PropertyDescriptor.Builder()
            .name("spark master")
            .description("-master <local|local[2]|local[*]|spark://HOST:PORT|yarn-cluster|yarn-master|mesos://HOST:PORT >")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

//    public static final PropertyDescriptor SPARK_OPTS = new PropertyDescriptor.Builder()
//            .name("spark options")
//            .description("The options for Spark-submit")
//            .build();

    public static final PropertyDescriptor JAR_FILES = new PropertyDescriptor.Builder()
            .name("application jars")
            .description("The jar files for applications, splited by comma")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ARGS = new PropertyDescriptor.Builder()
            .name("args")
            .description("The args for the spark applications")
            .required(true)
            .addValidator(Validator.VALID)
            .defaultValue("")
            .build();

//    public static final PropertyDescriptor EVN_PROPS = new PropertyDescriptor.Builder()
//            .name("env properties")
//            .description("The environment properties E.g., HADOOP_HOME, splitted by comma")
//            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("If spark jobs have been submitted successfully")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.LOG = getLogger();

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(SPARK_HOME);
        descriptors.add(MAIN_CLASS);
        descriptors.add(SPARK_MASTER);
//        descriptors.add(SPARK_OPTS);
        descriptors.add(JAR_FILES);
        descriptors.add(ARGS);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
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

    @OnStopped
    public void onStoped(final ProcessContext context) {
        isRunnable = true;
        try {
            if (sparkSubmit != null)
                sparkSubmit.waitFor();
        } catch (InterruptedException e) {
            LOG.error(e.toString());
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session)
            throws ProcessException {
        if (!isRunnable) {
            LOG.debug("This Processor can run only once after it starts!\n"
                    + "you can restart the processor to re-run the submission, ");
            return;
        }

        isRunnable = false;
        LOG.debug("To submit spark application");

        FlowFile flowFile = session.get();

        try {
            submitApplications(context, flowFile);
        } catch(Exception e) {
            e.printStackTrace();
        }

        flowFile = session.write(flowFile, new OutputStreamCallback() {
            @Override
            public void process(OutputStream out) throws IOException {

            }
        });
        session.transfer(flowFile, SUCCESS);
    }

    private Process submitApplications(final ProcessContext context, final FlowFile flowFile)
            throws InterruptedException, IOException {
        String sparkSubmitCMD =
                getPropertyValue(this.SPARK_HOME, context, flowFile) + "/bin/spark-submit";

        String generalARGs = getPropertyValue(this.ARGS, context, flowFile);
        if (generalARGs == null) generalARGs = "";

        String[] cmdArray = {
                sparkSubmitCMD,
                "--class",
                getPropertyValue(this.MAIN_CLASS, context, flowFile),
                "--master",
                getPropertyValue(this.SPARK_MASTER, context, flowFile),
                getPropertyValue(this.JAR_FILES, context, flowFile)
        };

        List<String> cmdList = new ArrayList<String>();
        for (int i = 0; i < cmdArray.length; i++) {
            cmdList.add(cmdArray[i]);
        }
        for (String s : generalARGs.split(" ")) {
            cmdList.add(s);
        }

        LOG.debug(cmdList.toString());
        ProcessBuilder pb = new ProcessBuilder(cmdList);
        
//        String envs = getPropertyValue(this.EVN_PROPS, context, flowFile);
//        if(envs !=null && !envs.equals("")) {
//            for(String env : envs.split(COMMA_SPLIT)) {
//                String[] kvs = env.trim().split(EQUAL_SPLIT);
//                if(kvs.length == 2)
//                    pb.environment().put(kvs[0], kvs[1]);
//            }
//        }
        
        pb.redirectErrorStream(true);
        this.sparkSubmit = pb.start();

        sparkSubmit.waitFor();

        InputStreamReader isr = new InputStreamReader(this.sparkSubmit.getInputStream());
        BufferedReader br = new BufferedReader(isr);

        String lineRead;
        while ((lineRead = br.readLine()) != null) {
            LOG.debug(lineRead);
        }

        return sparkSubmit;
    }

    private String getPropertyValue(PropertyDescriptor property, final ProcessContext context,
            final FlowFile flowfile) {
        return context.getProperty(property).evaluateAttributeExpressions(flowfile).getValue();
    }
}
