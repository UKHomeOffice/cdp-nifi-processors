/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import com.fasterxml.uuid.EthernetAddress;
import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import com.google.common.base.CharMatcher;
import com.pontusnetworks.utils.StringReplacer;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.hbase.HBaseClientService;
import org.apache.nifi.hbase.put.PutColumn;
import org.apache.nifi.hbase.scan.Column;
import org.apache.nifi.hbase.scan.ResultCell;
import org.apache.nifi.hbase.scan.ResultHandler;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * @author Leo Martins
 */

@TriggerSerially @InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)

@WritesAttributes({
    @WritesAttribute(attribute = "pontus.id.uuid", description = "The UUID associated with the incoming record"),
    @WritesAttribute(attribute = "pontus.id.type", description = "The type of UUID (NEW, EXISTING)"),
    @WritesAttribute(attribute = "pontus.match.status", description = "The status associated with the record "
        + "(MATCH, POTENTIAL_MATCH, MULTIPLE, MERGE, NO_MATCH) ") })

@Tags({ "pontus", "POLE", "GUID", "UUID", "HBASE" }) @CapabilityDescription(
    "Gets the incoming object, checks HBASE to see whether there's a matching record, and if there "
        + "is, returns its GUID; if there isn't, it creates a new GUID.") public class PontusIdGenerator
    extends AbstractProcessor
{

  final PropertyDescriptor HBASE_CLIENT_SERVICE = new PropertyDescriptor.Builder().name("HBase Client Service")
      .description("Specifies the Controller Service to use for accessing HBase.").required(true)
      .identifiesControllerService(HBaseClientService.class).build();
  final PropertyDescriptor UUID_NAMESPACE = new PropertyDescriptor.Builder()
      .name("UUID namespace (defaults to the mac address of the main NIC on the server")
      .defaultValue(EthernetAddress.fromInterface().toString()).required(true)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();
  final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder().name("Table Name")
      .description("The name of the HBase Table to put data into").required(true).expressionLanguageSupported(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

  //    static final Pattern COLUMNS_PATTERN = Pattern.compile("\\w+(:\\w+)?(?:,\\w+(:\\w+)?)*");
  final PropertyDescriptor FILTER_EXPRESSION = new PropertyDescriptor.Builder().name("Filter Expression").description(
      "An HBASE Filter String with replaceable tokens using ~~{attrib}~~  in the format of "
          + "https://www.cloudera.com/documentation/enterprise/5-5-x/topics/admin_hbase_filtering.html.</br>Examples:"
          + "</br> >, 'binary:abc' will match everything that is lexicographically greater than \"abc\"\n"
          + "</br> >, 'binary:~~{firstName}~~' will match everything that is lexicographically greater than the value of the flow file attribute \"firstName\"\n"
          + "</br> =, 'binaryprefix:abc' will match everything whose first 3 characters are lexicographically equal to \"abc\"\n"
          + "</br> !=, 'regexstring:ab*yz' will match everything that doesn't begin with \"ab\" and ends with \"yz\"\n"
          + "</br> !=, 'regexstring:ab*yz' will match everything that doesn't begin with \"ab\" and ends with \"yz\"\n"
          + "</br> =, 'substring:abc123' will match everything that begins with the substring \"abc123\"\n")
      .required(true).expressionLanguageSupported(false).addValidator(new Validator()
      {
        @Override public ValidationResult validate(String subject, String input, ValidationContext context)
        {
          ValidationResult.Builder builder = new ValidationResult.Builder();
          boolean isAscii = CharMatcher.ASCII.matchesAllOf(input);
          builder.valid(isAscii);
          builder.input(input);
          builder.subject(subject);

          if (!isAscii)
          {
            builder.explanation(
                "Found non-ascii characters in the string; perhaps you copied and pasted from a word doc or web site?");
          }

          ValidationResult res = builder.build();

          return res;
        }
      }).build();
  final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder().name("Character Set")
      .description("Specifies which character set is used to encode the data in HBase").required(true)
      .defaultValue("UTF-8").addValidator(StandardValidators.CHARACTER_SET_VALIDATOR).build();
  //    static final PropertyDescriptor COLUMNS = new PropertyDescriptor.Builder()
  //            .name("Columns")
  //            .description("A comma-separated list of \"<colFamily>:<colQualifier>\" pairs to return when scanning. To return all columns " +
  //                    "for a given family, leave off the qualifier such as \"<colFamily1>,<colFamily2>\".")
  //            .required(false)
  //            .expressionLanguageSupported(false)
  //            .addValidator(StandardValidators.createRegexMatchingValidator(COLUMNS_PATTERN))
  //            .build();
  final PropertyDescriptor FORCE_NEW_GUID = new PropertyDescriptor.Builder().name("Force new GUID")
      .description("Determines whether a new GUID is returned blindly without checking HBASE first.").required(false)
      .defaultValue("false").addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();
  final PropertyDescriptor PUT_OBJ_WITH_GUID = new PropertyDescriptor.Builder().name("Add Object").description(
      "Determines whether the resulting object should be added to HBASE using the attribute names as columns.")
      .required(true).defaultValue("true").allowableValues("true", "false")
      .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();
  final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
      .description("All FlowFiles are routed to this relationship").build();
  TimeBasedGenerator uuidGen = null;
  HBaseClientService hBaseClientService = null;
  String tableName = null;
  String charSet = null;
  String filterExpression = null;
  String rowKeyString = null;
  boolean forceNewGUID = false;
  boolean putObjWithGUID = true;
  private volatile List<Column> columns = Collections.EMPTY_LIST;
  private Pattern colonSplitPattern = Pattern.compile(":");

  private String createUUID()
  {
    UUID uuid = uuidGen.generate();

    return uuid.toString();
  }

  @Override public Set<Relationship> getRelationships()
  {
    return Collections.singleton(REL_SUCCESS);
  }

  @Override protected List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    final List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(HBASE_CLIENT_SERVICE);
    properties.add(TABLE_NAME);
    //        properties.add(COLUMNS);
    properties.add(CHARSET);
    properties.add(FILTER_EXPRESSION);
    properties.add(UUID_NAMESPACE);
    properties.add(FORCE_NEW_GUID);
    properties.add(PUT_OBJ_WITH_GUID);

    return properties;
  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {
    if (descriptor.equals(UUID_NAMESPACE))
    {
      uuidGen = null;
    }
    else if (descriptor.equals(HBASE_CLIENT_SERVICE))
    {
      hBaseClientService = null;
    }
    else if (descriptor.equals(TABLE_NAME))
    {
      tableName = newValue;
    }
    else if (descriptor.equals(FILTER_EXPRESSION))
    {
      filterExpression = newValue;
    }
    else if (descriptor.equals(CHARSET))
    {
      if (charSet == null)
      {
        charSet = "UTF-8";
      }
      else
      {
        charSet = newValue;
      }
    }
    //        }else if (descriptor.equals(COLUMNS)) {
    //            columns.clear();
    //        }
    else if (descriptor.equals(FORCE_NEW_GUID))
    {
      forceNewGUID = Boolean.parseBoolean(newValue);
    }
    else if (descriptor.equals(PUT_OBJ_WITH_GUID))
    {
      putObjWithGUID = Boolean.parseBoolean(newValue);
    }

  }

  //    @Override
  //    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
  ////        final String columns = validationContext.getProperty(COLUMNS).getValue();
  //
  //        final List<ValidationResult> problems = new ArrayList<>();
  //
  ////        if (StringUtils.isBlank(columns)) {
  ////            problems.add(new ValidationResult.Builder()
  ////                    .valid(false)
  ////                    .explanation("a filter expression can not be used in conjunction with the Columns property")
  ////                    .build());
  ////        }
  //
  //
  //
  //        return problems;
  //    }

  @OnScheduled public void parseProps(final ProcessContext context) throws IOException
  {

    if (uuidGen == null)
    {
      PropertyValue separatorCharPropVal = context.getProperty(UUID_NAMESPACE);
      EthernetAddress addr = EthernetAddress.valueOf(separatorCharPropVal.getValue());
      uuidGen = Generators.timeBasedGenerator(addr);
    }
    if (hBaseClientService == null)
    {
      hBaseClientService = context.getProperty(HBASE_CLIENT_SERVICE).asControllerService(HBaseClientService.class);
    }
    if (charSet == null)
    {
      charSet = context.getProperty(CHARSET).getValue();
      charSet = (charSet == null) ? "UTF-8" : charSet;
    }
    //        if (this.columns.size() == 0) {
    //
    //            final String columnsValue = context.getProperty(COLUMNS).getValue();
    //            final String[] columnsStrs = (columnsValue == null || columnsValue.isEmpty() ? new String[0] : columnsValue.split(","));
    //
    //            this.columns.clear();
    //            for (final String column : columnsStrs) {
    //                if (column.contains(":")) {
    //                    final String[] parts = commaSplitPattern.split(column);
    //                    final byte[] cf = parts[0].getBytes(Charset.forName(charSet));
    //                    final byte[] cq = parts[1].getBytes(Charset.forName(charSet));
    //                    this.columns.add(new Column(cf, cq));
    //                } else {
    //                    final byte[] cf = column.getBytes(Charset.forName(charSet));
    //                    this.columns.add(new Column(cf, null));
    //                }
    //            }
    //
    //        }
    if (filterExpression == null)
    {
      filterExpression = context.getProperty(FILTER_EXPRESSION).getValue();
    }
    if (tableName == null)
    {
      tableName = context.getProperty(TABLE_NAME).getValue();

    }
  }

  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {
    final ComponentLog log = this.getLogger();
    final FlowFile flowfile = session.get();
    final Map<String, String> attributes = new HashMap<>();

    if (forceNewGUID)
    {
      rowKeyString = createUUID();
      attributes.put("pontus.id.uuid", rowKeyString);
      attributes.put("pontus.id.type", UUIDType.NEW.toString());
      attributes.put("pontus.match.status", MatchStatus.NO_MATCH.toString());
      FlowFile localFlowFile = flowfile;
      localFlowFile = session.putAllAttributes(localFlowFile, attributes);
      session.transfer(localFlowFile, REL_SUCCESS);
      session.commit();
      return;
    }

    final Map<String, String> replacementMap = flowfile.getAttributes();

    final String startingFilter = StringReplacer
        .replaceTokensEscapeSingleQuotes(replacementMap, filterExpression, 5, "\\");
    // we need to escape single quotes with a leading single quote, but because the escape char is the same as the single quote,
    // we need to do this in two steps.
    final String filter = StringReplacer.replaceAll(startingFilter, "\\", "'");

    final Set<String> uuidSet = new HashSet<>();

    rowKeyString = null;

    try
    {
      hBaseClientService.scan(tableName, columns, filter, 0, new ResultHandler()
      {
        @Override public void handle(final byte[] rowKey, final ResultCell[] resultCells)
        {
          rowKeyString = new String(rowKey, StandardCharsets.UTF_8);
          uuidSet.add(rowKeyString);
        }
      });

      int numMatches = uuidSet.size();

      String provenanceReporterUri;
      if (numMatches == 1)
      {
        attributes.put("pontus.id.uuid", rowKeyString);
        attributes.put("pontus.id.type", UUIDType.EXISTING.toString());
        attributes.put("pontus.match.status", MatchStatus.MATCH.toString());

        provenanceReporterUri = ("hbase://" + tableName + "/?" + filter + "/" + rowKeyString);
      }
      else
      {
        rowKeyString = createUUID();
        attributes.put("pontus.id.uuid", rowKeyString);
        attributes.put("pontus.id.type", UUIDType.NEW.toString());
        attributes.put("pontus.match.status",
            (numMatches > 1) ? MatchStatus.MULTIPLE.toString() : MatchStatus.NO_MATCH.toString());

        provenanceReporterUri = ("guid://" + rowKeyString);

      }
      FlowFile localFlowFile = session.putAllAttributes(flowfile, attributes);

      session.getProvenanceReporter().receive(localFlowFile, provenanceReporterUri); // );

      if (putObjWithGUID)
      {

        Map<String, String> putAttribs = localFlowFile.getAttributes();

        List<PutColumn> cols = new ArrayList<>(putAttribs.size());

        Set<Map.Entry<String, String>> putAttribsEntrySet = putAttribs.entrySet();

        int colCount = 0;
        for (Map.Entry<String, String> entry : putAttribsEntrySet)
        {
          String key = entry.getKey();
          int delim = key.indexOf(':');

          if (delim >= 0)
          {
            PutColumn col = new PutColumn(key.substring(0, delim).getBytes(), key.substring(delim + 1).getBytes(),
                entry.getValue().getBytes());
            cols.add(colCount, col);
          }
        }
        //                session.getProvenanceReporter().send(localFlowFile,)
        hBaseClientService.put(tableName, rowKeyString.getBytes(), cols);

      }

      session.transfer(localFlowFile, REL_SUCCESS);
      session.commit();

      return;

    }
    catch (IOException e)
    {
      log.error("Failed to receive data from HBase due to {}", e);

    }
    catch (IllegalArgumentException e)
    {
      log.error("Failed to receive data from HBase due to an illegal filter; look at "
          + "https://issues.apache.org/jira/browse/HBASE-4176 for the filter language; error: {}", e);
    }

    session.rollback();
    return;

  }

  public static enum MatchStatus
  {
    MATCH, POTENTIAL_MATCH, MULTIPLE, MERGE, NO_MATCH
  }

  public static enum UUIDType
  {
    NEW, EXISTING
  }

}
