package com.pontusvision.nifi.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.mbox.OutlookPSTParser;
import org.apache.tika.sax.ToTextContentHandler;
import org.xml.sax.ContentHandler;

import java.io.IOException;
import java.util.*;

//import java.util.*;
@TriggerSerially @SupportsBatching @InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
//@SideEffectFree
@Tags({ "Pontus", "MSOffice", "Outlook",
    "PST" }) @CapabilityDescription("Reads data from PST Files serialized in a flow file, and creates smaller flow files with either the text or individual e-mails.")

public class PontusMSOfficePSTReaderRecord extends PontusMSOfficeTextExtractor
{

  public static final PropertyDescriptor EMAIL_HEADERS_FILTER_REGEX = new PropertyDescriptor.Builder().name("Email Headers regex")
      .description("Specifies the filter to match e-mail message headers that will be processed.").required(false)
      .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR).defaultValue(".*").build();





  @Override protected void init(final ProcessorInitializationContext context)
  {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(ADD_CONTENT_TO_ATTRIB);
    descriptors.add(ADD_CONTENT_TO_FLOWFILE);
    descriptors.add(CONTENT_FORMATTER);
    descriptors.add(EMAIL_HEADERS_FILTER_REGEX);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<>();
    relationships.add(SUCCESS);
    relationships.add(FAILURE);
    relationships.add(PARSED);
    this.relationships = Collections.unmodifiableSet(relationships);

    logger = context.getLogger();

  }

  @Override public Set<Relationship> getRelationships()
  {
    return this.relationships;
  }



  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {
    FlowFile flowFile = session.get();
    if (flowFile == null)
    {
      return;
    }

    final Map<String, String> attributes = new HashMap<>();

    final FlowFile original = flowFile;
    final Map<String, String> originalAttributes = flowFile.getAttributes();
    attributes.putAll(originalAttributes);

    try
    {
      final FlowFile tempFlowFile = flowFile;

      session.read(flowFile, in -> {
        try
        {
          OutlookPSTParser parser = new OutlookPSTParser();
          Metadata parserMetadata = new Metadata();
          ContentHandler parserHandler = new ToTextContentHandler();

          ParseContext parserContext = new ParseContext();

          EmbeddedTrackingExtrator trackingExtrator = new EmbeddedTrackingExtrator(parserContext, session,
              originalAttributes);
          parserContext.set(EmbeddedDocumentExtractor.class, trackingExtrator);
          parserContext.set(Parser.class, new AutoDetectParser());

          parser.parse(in, parserHandler, parserMetadata, parserContext);

        }
        catch (Exception ex)

        {
          ex.printStackTrace();
          logger.error("Failed to read PST File: ", ex);
          //          session.transfer(original, FAILURE);
          throw new IOException(ex);
        }
      });

      session.transfer(original, SUCCESS);

      //      session.remove(original);

    }
    catch (Throwable t)
    {
      FlowFile localFlowFile = original;
      localFlowFile = session.putAllAttributes(localFlowFile, attributes);
      session.transfer(localFlowFile, FAILURE);

    }
  }

}
