package com.pontusvision.nifi.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.microsoft.OfficeParser;
import org.apache.tika.sax.ToTextContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.apache.commons.lang3.StringEscapeUtils.escapeJson;

//import java.util.*;
@TriggerSerially @SupportsBatching @InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
//@SideEffectFree
@Tags({ "Pontus", "MSOffice", "doc", "docx", "xlsx", "xls" })
@CapabilityDescription("Reads data from Office Files serialized in a flow file, and creates flow files with the text either as the content or as an attribute.")

public class PontusMSOfficeTextExtractor extends AbstractProcessor
{

  protected class EmbeddedTrackingExtrator extends ParsingEmbeddedDocumentExtractor
  {
    //    List<Metadata> trackingMetadata = new ArrayList<>();
    final ProcessSession session;
    Map<String, String> currAttribs;

    public EmbeddedTrackingExtrator(ParseContext context, ProcessSession session, Map<String, String> currAttribs)
    {
      super(context);
      this.session = session;
      this.currAttribs = currAttribs;
    }

    @Override public boolean shouldParseEmbedded(Metadata metadata)
    {
      return true;
    }

    @Override public void parseEmbedded(InputStream stream, ContentHandler ch, Metadata metadata, boolean outputHtml)
        throws SAXException, IOException
    {
      ToTextContentHandler parserHandler = new ToTextContentHandler();
      super.parseEmbedded(stream, parserHandler, metadata, false);
      FlowFile flowfile = session.create();

      String[] names = metadata.names();

      HashMap<String, String> attributes = new HashMap<>(names.length + currAttribs.size());

      attributes.putAll(currAttribs);

      for (int i = 0, ilen = names.length; i < ilen; i++)
      {
        String val = metadata.get(names[i]);
        attributes.put(names[i], val);
      }

      String data = parserHandler.toString();

      if (formatContent)
      {
        data = String.format(formatContentPattern, escapeJson(data));
      }

      if (addContentToAttribute)
      {
        attributes.put("CONTENT_ATTRIB", data);
      }

      flowfile = session.putAllAttributes(flowfile, attributes);

      if (!addContentToFlowFile)
      {

        final String contentStr = data;
        flowfile = session.write(flowfile, out -> out.write(contentStr.getBytes()));
      }

      parserHandler.endDocument();
      session.transfer(flowfile, PARSED);

    }
  }

  public static final PropertyDescriptor ADD_CONTENT_TO_FLOWFILE = new PropertyDescriptor.Builder().name("Add content to attribute")
      .description(
          "Specifies whether the contents of each email should be added to a CONTENT_ATTRIBUTE.  If set to false, the e-mail contents are sent in the flow file.")
      .required(false).addValidator(StandardValidators.BOOLEAN_VALIDATOR).defaultValue("true").build();

  public static final PropertyDescriptor ADD_CONTENT_TO_ATTRIB = new PropertyDescriptor.Builder().name("Add content to attribute")
      .description(
          "Specifies whether the contents of each email should be added to a CONTENT_ATTRIBUTE.  If set to false, the e-mail contents are sent in the flow file.")
      .required(false).addValidator(StandardValidators.BOOLEAN_VALIDATOR).defaultValue("true").build();

  public static String CONTENT_FORMATTER_DEFAULT = "{\"text\":\"%s\","
      + "\"features\":{\"entities\":{}}}";

  public static final PropertyDescriptor CONTENT_FORMATTER = new PropertyDescriptor.Builder().name("Content String Formatter")
      .description(
          "Add the contents with special formatting; only one %s placeholder is allowed, and is replaced with the text; if set to an empty string, no formatting is done.")
      .required(false).addValidator((subject, input, context) -> {
        final boolean isValid;
        String explanation = "";
        if (input == null || input.length() == 0)
        {
          isValid = true;
        }
        else if (!input.contains("%s"))
        {
          isValid = false;
          explanation = "Could not find any %s replacements; the value must either be empty, or contain only one %s to be used as a replacement";
        }
        else if (input.indexOf("%s") != input.lastIndexOf("%s"))
        {
          isValid = false;
          explanation = "Found more than one %s replacement; the value must either be empty, or contain only one %s to be used as a replacement";

        }
        else
        {
          isValid = true;
        }

        return new ValidationResult.Builder().subject(subject).input(input).valid(isValid).explanation(explanation)
            .build();
      }).defaultValue(CONTENT_FORMATTER_DEFAULT).build();


  public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
      .description("Success relationship").build();

  public static final Relationship PARSED = new Relationship.Builder().name("PARSED")
      .description("Successfully parsed files").build();

  public static final Relationship FAILURE = new Relationship.Builder().name("FAILURE")
      .description("Failure relationship").build();

  protected List<PropertyDescriptor> descriptors;

  protected Set<Relationship> relationships;

  protected ComponentLog logger;

  protected boolean addContentToAttribute = true;
  protected boolean addContentToFlowFile = true;
  protected boolean formatContent = true;
  protected String formatContentPattern = CONTENT_FORMATTER_DEFAULT;

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {
    if (descriptor.equals(ADD_CONTENT_TO_ATTRIB))
    {

      addContentToAttribute = Boolean.parseBoolean(newValue);
    }
    else if (descriptor.equals(ADD_CONTENT_TO_FLOWFILE))
    {

      addContentToFlowFile = Boolean.parseBoolean(newValue);
    }

    else if (descriptor.equals(CONTENT_FORMATTER))
    {

      formatContentPattern = newValue;
      formatContent = formatContentPattern.length() > 0;
    }

  }

  @Override protected void init(final ProcessorInitializationContext context)
  {
    final List<PropertyDescriptor> descriptors = new ArrayList<>();
    descriptors.add(ADD_CONTENT_TO_ATTRIB);
    descriptors.add(ADD_CONTENT_TO_FLOWFILE);
    descriptors.add(CONTENT_FORMATTER);
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

  @Override public final List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    return descriptors;
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
          OfficeParser parser = new OfficeParser();

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
