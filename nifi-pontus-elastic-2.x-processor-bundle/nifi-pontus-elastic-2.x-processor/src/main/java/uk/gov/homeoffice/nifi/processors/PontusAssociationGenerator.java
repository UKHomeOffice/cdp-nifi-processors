/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package uk.gov.homeoffice.nifi.processors;

import com.pontusnetworks.utils.StringReplacer;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.elasticsearch.action.bulk.BulkAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.NoNodeAvailableException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author phillip
 */

@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)

@WritesAttributes({
  @WritesAttribute(attribute = "pontus.id.uuid", description = "The UUID associated with the incoming record"),
  @WritesAttribute(attribute = "pontus.id.type", description = "The type of UUID (NEW, EXISTING)"),
  @WritesAttribute(attribute = "pontus.match.status", description = "The status associated with the record " +
    "(MATCH, POTENTIAL_MATCH, MULTIPLE, MERGE, NO_MATCH) ")
})

@Tags({"pontus", "POLE", "GUID", "UUID", "elasticsearch", "association"})
@CapabilityDescription("Creates a new association in Elastic Search and returns the new Row ID ")
public class PontusAssociationGenerator extends PontusElasticIdGenerator  {

//
//  static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
//    .description("All FlowFiles that are written to Elasticsearch are routed to this relationship").build();
//
//  static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
//    .description("All FlowFiles that cannot be written to Elasticsearch are routed to this relationship").build();
//
//  static final Relationship REL_RETRY = new Relationship.Builder().name("retry")
//    .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
//    .build();


  final PropertyDescriptor SOURCE_ID = new PropertyDescriptor.Builder()
    .name("Inbound Source ID Attribute")
    .description("Specifies the  41-character GUID+(first5 chars of the md5sum of the GUID) -- DON'T ASK :(  for the source association.")
    .required(true)
    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
    .build();

  final PropertyDescriptor TARGET_ID = new PropertyDescriptor.Builder()
    .name("Inbound Target ID Attribute")
    .description("Specifies the  41-character GUID+(first5 chars of the md5sum of the GUID) -- DON'T ASK :(  for the target association.")
    .required(true)
    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
    .build();


  final PropertyDescriptor RELATIONSHIP = new PropertyDescriptor.Builder()
    .name("Relationship ")
    .description("Description of the relationship for this association (e.g. ownsVehicle,ownsAddress,revwEvdence,crtdEvdence,ANPR,MANIFEST_API,VISA_APPLICATION,CROSSED BORDER")
    .required(true)
    .allowableValues("ownsVehicle", "ownsAddress", "revwEvdence", "crtdEvdence", "ANPR", "MANIFEST_API", "VISA_APPLICATION", "CROSSED BORDER")
    .defaultValue("ANPR")
    .build();


  final PropertyDescriptor IS_INVERTED = new PropertyDescriptor.Builder()
    .name("Is Inverted ")
    .description("Boolean of whether or this is an 'Inverted' association (again, don't ask :(  ).")
    .required(true)
    .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
    .allowableValues("true", "false")
    .build();

  final PropertyDescriptor TARGET_TYPE = new PropertyDescriptor.Builder()
    .name("Target type")
    .description("Specifies source type of the association  (e.g. Person, Object, Location, Event).")
    .required(true)
    .allowableValues("Person", "Object", "Location", "Event", "Identity", "Status")
    .build();

  final PropertyDescriptor TARGET_SUBTYPE = new PropertyDescriptor.Builder()
    .name("Target Sub type ")
    .description("(e.g. ORGANISATIONIDENTITY, PERSONIDENTITY, VEHICLE, ADDRESS, APIMANIFEST, etc) .")
    .required(true)
    .allowableValues("ORGANISATIONIDENTITY", "PERSONIDENTITY", "VEHICLE", "ADDRESS", "APIMANIFEST")
    .build();
  final PropertyDescriptor SOURCE_TYPE = new PropertyDescriptor.Builder()
    .name("Source type")
    .description("Specifies source type of the association  (e.g. Person, Object, Location, Event).")
    .required(true)
    .allowableValues("Person", "Object", "Location", "Event", "Identity", "Status")
    .build();

  final PropertyDescriptor SOURCE_SUBTYPE = new PropertyDescriptor.Builder()
    .name("Source Sub type ")
    .description("(e.g. ORGANISATIONIDENTITY, PERSONIDENTITY, VEHICLE, ADDRESS, APIMANIFEST, etc) .")
    .required(true)
    .allowableValues("ORGANISATIONIDENTITY", "PERSONIDENTITY", "VEHICLE", "ADDRESS", "APIMANIFEST")
    .build();


  final PropertyDescriptor INDEX = new PropertyDescriptor.Builder()
    .name("Index")
    .description("The name of the index to insert into")
    .required(true)
    .expressionLanguageSupported(true)
    .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
      AttributeExpression.ResultType.STRING, true))
    .build();

  final PropertyDescriptor TYPE = new PropertyDescriptor.Builder()
    .name("Type")
    .description("The type of this document (used by Elasticsearch for indexing and searching)")
    .required(true)
    .expressionLanguageSupported(true)
    .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
      AttributeExpression.ResultType.STRING, true))
    .build();

  final PropertyDescriptor INDEX_OP = new PropertyDescriptor.Builder()
    .name("Index Operation")
    .description("The type of the operation used to index (index, update, upsert)")
    .required(true)
    .expressionLanguageSupported(true)
    .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(
      AttributeExpression.ResultType.STRING, true))
    .defaultValue("index")
    .build();



  String index = null;

  String type = null;
  String outputPrefix = "pontus";

  String uuidAttrib = outputPrefix + "id.uuid";

  String source_id;
  String source_type;
  String source_subtype;
  String target_id;
  String target_type;
  String target_subtype;
  String relationship;
  boolean is_inverted;



  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    final List<PropertyDescriptor> properties = new ArrayList<>();

    properties.add(SOURCE_ID);
    properties.add(SOURCE_TYPE);
    properties.add(SOURCE_SUBTYPE);
    properties.add(TARGET_ID);
    properties.add(TARGET_TYPE);
    properties.add(TARGET_SUBTYPE);
    properties.add(RELATIONSHIP);
    properties.add(IS_INVERTED);

    properties.add(ENABLE_KERBEROS);

    properties.add(KERBEROS_ACCEPTOR_PRINCIPAL);
    properties.add(KERBEROS_INITIATOR_PRINCIPAL);
    properties.add(KERBEROS_PASSWORD);

    properties.add(KERBEROS_KEYTAB);


//    properties.add(TABLE_NAME);
//        properties.add(COLUMNS);
    properties.add(CHARSET);

    properties.add(CLUSTER_NAME);
    properties.add(HOSTS);
    properties.add(PROP_SSL_CONTEXT_SERVICE);
    properties.add(PROP_SHIELD_LOCATION);
    properties.add(ELASTIC_YML_CONF);
//    properties.add(USERNAME);
//    properties.add(PASSWORD);
    properties.add(PING_TIMEOUT);
    properties.add(SAMPLER_INTERVAL);
    properties.add(INDEX);
    properties.add(TYPE);
    properties.add(CHARSET);
    properties.add(INDEX_OP);


    return properties;
  }

  @Override
  public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {

    if (descriptor.equals(INDEX)) {
      index = newValue;
    }
    else if (descriptor.equals(TYPE)) {
      type = newValue;
    }

    else if (descriptor.equals(SOURCE_ID)) {
      source_id = newValue;
    }
    else if (descriptor.equals(SOURCE_TYPE)) {
      source_type = newValue;

    }
    else if (descriptor.equals(SOURCE_SUBTYPE)) {
      source_subtype = newValue;
    }
    else if (descriptor.equals(TARGET_ID)) {
      target_id = newValue;
    }
    else if (descriptor.equals(TARGET_TYPE)) {
      target_type = newValue;
    }
    else if (descriptor.equals(TARGET_SUBTYPE)) {
      target_subtype = newValue;

    }
    else if (descriptor.equals(RELATIONSHIP)) {
      relationship = newValue;
    }
    else if (descriptor.equals(IS_INVERTED)) {
      is_inverted = Boolean.parseBoolean(newValue);
    }


    else if (descriptor.equals(KERBEROS_INITIATOR_PRINCIPAL)) {
      kerberosInitiatorPrincipal = newValue;
//      kerberizedClient = null;
    }
    else if (descriptor.equals(KERBEROS_ACCEPTOR_PRINCIPAL)) {
      kerberosAcceptorPrincipal = newValue;
//      kerberizedClient = null;
    }
    else if (descriptor.equals(KERBEROS_KEYTAB)) {
      kerberosKeytab = newValue;
//      kerberizedClient = null;
    }

    else if (descriptor.equals(CHARSET)) {
      if (charSet == null) {
        charSet = "UTF-8";
      }
      else {
        charSet = newValue;
      }
    }
//        }else if (descriptor.equals(COLUMNS)) {
//            columns.clear();
//        }
    else if (descriptor.equals(ELASTIC_YML_CONF)) {
      elasticSearchYmlPathStr = newValue;
    }


  }

  @OnScheduled
  public void parseProps(final ProcessContext context) throws IOException {

//    if (kerberizedClient == null) {
//      kerberizedClient = getKerberizedClient(context, false);
//    }
  }

  public static String addBulkRequest(AbstractClient kerberizedClient, BulkRequestBuilder brb, String sourceId, String targetId, String sourceType, String sourceSubtype, String targetType, String targetSubtype, String relationship, boolean isInverted)
  {
    StringBuilder sb = new StringBuilder(sourceId);

    String rowKeyString = sb.append("^")
      .append(System.currentTimeMillis()).append("^")
      .append(targetId).toString();




    sb.setLength(0);

    sb.append("{");
    int colCount = 0;

    sb.append("\"sourceId\":").append("\":\"").append(sourceId).append("\",");
    sb.append("\"sourceType\":").append("\":\"").append(sourceType).append("\",");
    sb.append("\"sourceSubtype\":").append("\":\"").append(sourceSubtype).append("\",");
    sb.append("\"targetId\":").append("\":\"").append(targetId).append("\",");
    sb.append("\"targetType\":").append("\":\"").append(targetType).append("\",");
    sb.append("\"targetSubtype\":").append("\":\"").append(targetSubtype).append("\",");
    sb.append("\"relationship\":").append("\":\"").append(relationship).append("\",");
    sb.append("\"isInverted\":").append("\":\"").append(isInverted).append("\"");
    sb.append("}");
//                session.getProvenanceReporter().send(localFlowFile,)
//        hBaseClientService.put(tableName, rowKeyString.getBytes(), cols);


    IndexRequestBuilder irb = new IndexRequestBuilder(kerberizedClient, IndexAction.INSTANCE);

    IndexRequest ir = irb.setId(rowKeyString).setSource(sb.toString()).request();
    brb.add(ir);

    return rowKeyString;
  }


  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    final ComponentLog log = this.getLogger();
    final FlowFile flowfile = session.get();
    final Map<String, String> attributes = new HashMap<>();
    AbstractClient  kerberizedClient = null;



    final Map<String, String> attribs = flowfile.getAttributes();

    FlowFile localFlowFile = session.putAllAttributes(flowfile, attribs);

    String user = null;
    try {
      user = context.getProperty(KERBEROS_INITIATOR_PRINCIPAL).evaluateAttributeExpressions(flowfile).getValue();
      String passwd = context.getProperty(KERBEROS_PASSWORD).evaluateAttributeExpressions(flowfile).getValue();

      kerberizedClient = getKerberizedClient(context,user, passwd,false);

      String sourceId = attribs.get(source_id);
      String sourceType = attribs.get(source_type);
      String sourceSubtype = attribs.get(source_subtype);
      String targetId = attribs.get(target_id);
      String targetType = attribs.get(target_type);
      String targetSubtype = attribs.get(target_subtype);

      if (sourceId == null){
        session.transfer(localFlowFile, REL_FAILURE);
        log.error("Could not find source id in attribute "+ source_id.toString());
        return;
      }
      if (targetId == null){
        session.transfer(localFlowFile, REL_FAILURE);
        log.error("Could not find target id in attribute "+ target_id.toString());
        return;
      }




      BulkRequestBuilder brb = new BulkRequestBuilder(kerberizedClient, BulkAction.INSTANCE);
      ArrayList<String> uuids = null;


      if (sourceId.startsWith("[") && sourceId.endsWith("]") ){
        String[] sourceIds = sourceId.substring(1, sourceId.length() - 1).split(",");
        String[] targetIds = targetId.substring(1, targetId.length() - 1).split(",");

        if (sourceIds.length != targetIds.length) {

          session.transfer(localFlowFile, REL_FAILURE);
          log.error("The number of Source and Target IDs must match; got " + sourceIds.length + " source Ids, and " + targetIds.length + " target Ids.");
          return;
        }

        uuids = new ArrayList<>(sourceIds.length);

        for (int i = 0, ilen = sourceIds.length; i < ilen; i++) {
          String srcId = StringReplacer.replaceAll(sourceIds[i]," ","");
          String tgtId = StringReplacer.replaceAll(targetIds[i]," ","");
          uuids.add(addBulkRequest(kerberizedClient, brb, srcId, tgtId, sourceType, sourceSubtype, targetType, targetSubtype, relationship, is_inverted));
        }
      }
      else
      {
        uuids.add(addBulkRequest(kerberizedClient, brb, sourceId, targetId, sourceType, sourceSubtype, targetType, targetSubtype, relationship, is_inverted));
      }


      kerberizedClient.bulk(brb.request());
      Map<String, String> newAttribs = new HashMap<>();
      newAttribs.put(uuidAttrib, uuids.toString());
      localFlowFile = session.putAllAttributes(localFlowFile, newAttribs);
      session.transfer(localFlowFile, REL_SUCCESS);
      session.commit();

      return;


    } catch (NoNodeAvailableException e) {

      cleanupKerberosClient(user,kerberizedClient);
      log.error("Failed to receive data from Elastic error: {}", e);

    } catch (Exception e) {
      log.error("Failed to receive data from Elastic error: {}", e);
    }
    session.transfer(localFlowFile, REL_FAILURE);

    session.rollback();
    return;


  }

}
