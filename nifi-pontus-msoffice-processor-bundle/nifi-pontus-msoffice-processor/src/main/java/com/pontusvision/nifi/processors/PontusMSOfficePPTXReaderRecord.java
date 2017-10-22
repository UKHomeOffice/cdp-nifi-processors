package com.pontusvision.nifi.processors;

import com.google.gson.Gson;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.poi.sl.usermodel.PlaceableShape;
import org.apache.poi.sl.usermodel.ShapeContainer;
import org.apache.poi.xslf.usermodel.*;

import java.awt.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

//import java.util.*;
@TriggerSerially
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
//@SideEffectFree
@Tags({"MSOffice", "record", "Writer",  "PowerPoint" })
@WritesAttributes({
        @WritesAttribute(attribute = "processed.record.count", description = "The number of records processed")
})
@CapabilityDescription("Reads data from various MSOffice file formats, and writes them to a Record Writer Controller Service.  ")


public class PontusMSOfficePPTXReaderRecord extends AbstractProcessor {

//    final PropertyDescriptor TINKERPOP_CLIENT_CONF_FILE_URI = new PropertyDescriptor.Builder()
//            .name("Tinkerpop Client configuration URI")
//            .description("Specifies the configuration file to configure this connection to tinkerpop.")
//            .required(false)
//            .addValidator(StandardValidators.URI_VALIDATOR)
////            .identifiesControllerService(HBaseClientService.class)
//            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Set Writer")
            .description("Specifies the Controller Service to use for writing incoming data")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    final PropertyDescriptor SLIDE_TITLES_REGEX = new PropertyDescriptor.Builder()
            .name("Slide titles regex")
            .description("Specifies the filter to match slide titles that will be processed.")
            .required(false)
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .defaultValue(".*")
            .build();

    final PropertyDescriptor SHAPE_TYPE_TO_RECORD_WRITER_JSON_MAP = new PropertyDescriptor.Builder()
            .name("Shape type to Record Writer Field Mapping")
            .description("specifies a JSON Array that maps shape types to fields of the record writer.")
            .required(true)
            .defaultValue("{'RoundedRectangle-555555': 'server', \n" +
                    "'Triangle':'switch' }")
            .addValidator((subject, input, context) -> {
                try {
                    Map<String, String> result = new Gson().fromJson(input, Map.class);
                    final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);


                    for (Map.Entry<String,String> eset :result.entrySet()){
                        String outputVal = eset.getValue();
                    }
                }
                catch(Throwable t){
                    return (new ValidationResult.Builder()).subject(subject).input(input).valid(false).explanation(t.getMessage()).build();

                }
                return (new ValidationResult.Builder()).subject(subject).input(input).valid(true).build();


            })
            .defaultValue(".*")
            .build();


    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure relationship")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(RECORD_WRITER);
//        properties.remove( TINKERPOP_QUERY_PARAM_PREFIX);

        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }


        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final Map<String, String> attributes = new HashMap<>();
        final AtomicInteger recordCount = new AtomicInteger();

        final List<String> reqUUIDs = new LinkedList<>();

        final FlowFile original = flowFile;
        final Map<String, String> originalAttributes = flowFile.getAttributes();
        attributes.putAll(originalAttributes);


        try {
            final FlowFile tempFlowFile = flowFile;

            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream in, final OutputStream out) throws IOException {

                    XMLSlideShow ppt = new XMLSlideShow(in);
                    List<XSLFSlide> slides = ppt.getSlides();

                    for (int i = 0, ilen = slides.size(); i < ilen; i++) {
                        XSLFSlide slide = slides.get(i);
                        List<XSLFShape> shapes = slide.getShapes();
                        Set<String> idsProcessed = new HashSet<>();
                        for (XSLFShape sh : shapes) {

                            // name of the shape
                            String name = sh.getShapeName();
                            ShapeContainer container = sh.getParent();
                            Color col = sh.getSheet().getBackground().getFillColor();
                            int rgb = col.getRGB();


                            // shapes's anchor which defines the position of this shape in the slide
                            if (sh instanceof PlaceableShape) {
                                PlaceableShape shape = ((PlaceableShape) sh);


                            }

                            if (sh instanceof XSLFConnectorShape) {
                                XSLFConnectorShape line = (XSLFConnectorShape) sh;
                                // work with Line
                            } else if (sh instanceof XSLFTextShape) {
                                XSLFTextShape shape = (XSLFTextShape) sh;
                                // work with a shape that can hold text
                            } else if (sh instanceof XSLFPictureShape) {
                                XSLFPictureShape shape = (XSLFPictureShape) sh;
                                // work with Picture
                            }
                        }
                    }

                    attributes.put("reqUUIDs", reqUUIDs.toString());
                    attributes.put("processed.record.count", String.valueOf(reqUUIDs.size()));
                    attributes.put("requested.record.count", String.valueOf(recordCount.get()));

                    FlowFile localFlowFile = original;
                    localFlowFile = session.putAllAttributes(localFlowFile, attributes);
                    session.transfer(localFlowFile, SUCCESS);

                }

            });
        }catch (Throwable t){
            FlowFile localFlowFile = original;
            localFlowFile = session.putAllAttributes(localFlowFile, attributes);
            session.transfer(localFlowFile, FAILURE);

        }
    }

}
