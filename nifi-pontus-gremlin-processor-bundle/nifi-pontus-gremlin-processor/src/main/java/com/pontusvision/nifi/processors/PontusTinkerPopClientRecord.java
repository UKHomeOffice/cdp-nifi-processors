package com.pontusvision.nifi.processors;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

//import java.util.*;
@TriggerSerially @SupportsBatching @InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
//@SideEffectFree
@Tags({ "GDPR", "record", "Write", "schema", "tinkerpop", "gremlin", "json", "csv", "avro", "log", "logs", "freeform",
    "text" }) @WritesAttributes({
    @WritesAttribute(attribute = "processed.record.count", description = "The number of records processed"),
    @WritesAttribute(attribute = "requested.record.count", description = "The number of records processed") }) @CapabilityDescription(
    "Reads data from a Record Reader Controller Service, and puts it into a TinkerPop Gremlin Server using a given query.  "
        + "Each of the schema fields is passed as a variable to the tinkerpop query string.  "
        + "As an example, if the server has a graph created as g1, then we can create an alias pointing \"g1\" to \"g\" by using the alias option."
        + "Then, the tinkerpop query looks like this:\n"
        + "'v1 = g.addV(\"person\").property(id, userID1).property(\"name\", userName1).property(\"age\", userAge1).next()\n"
        + "v2 = g.addV(\"software\").property(id, userID2).property(\"name\", userName2).property(\"lang\", userLang2).next()\n"
        + "g.addE(\"created\").from(v1).to(v2).property(id, relId1).property(\"weight\", relWeight1)\n', then the "
        + "variables userID1, userName1, userID2, userName2, userLang2, relId1, relWeight1 would all be taken from the Record Reader's "
        + "schema for each record.")

public class PontusTinkerPopClientRecord extends PontusTinkerPopClient
{
  static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder().name("record-reader")
      .displayName("Record Reader").description("Specifies the Controller Service to use for reading incoming data")
      .identifiesControllerService(RecordReaderFactory.class).required(true).build();

  @Override protected List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
    properties.add(RECORD_READER);
    //        properties.remove( TINKERPOP_QUERY_PARAM_PREFIX);

    return properties;
  }



  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {
    FlowFile flowFile = session.get();
    if (flowFile == null)
    {
      return;
    }

    final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER)
        .asControllerService(RecordReaderFactory.class);

    final Map<String, String> attributes = new HashMap<>();
    final AtomicInteger recordCount = new AtomicInteger();

    final List<String> reqUUIDs = new LinkedList<>();


    Map<String, String> allAttribs = flowFile.getAttributes();

    final Map<String, Object> tinkerpopFlowFileAttribs = allAttribs.entrySet().stream()
        .filter((entry -> entry.getKey().startsWith(queryAttribPrefixStr)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    final FlowFile original = flowFile;
    final Map<String, String> originalAttributes = flowFile.getAttributes();
    attributes.putAll(originalAttributes);

    try
    {
      final FlowFile tempFlowFile = flowFile;

      flowFile = session.write(flowFile, new StreamCallback()
      {
        @Override public void process(final InputStream in, final OutputStream out) throws IOException
        {

          //                    try (final RecordReader reader = readerFactory.createRecordReader(originalAttributes, in, getLogger())) {
          try (final RecordReader reader = readerFactory.createRecordReader(original, in, getLogger()))
          {

            final RecordSchema rSchema = reader.getSchema();

            List<RecordField> fields = rSchema.getFields();

            Record record;
            Map<String, Object> tinkerpopAttribs = new HashMap<>(fields.size() + tinkerpopFlowFileAttribs.size());

            List<CompletableFuture<ResultSet>> resSets = new LinkedList<>();
            int resetLen = queryAttribPrefixStr.length();
            StringBuilder sb = new StringBuilder(queryAttribPrefixStr);

            while ((record = reader.nextRecord()) != null)
            {

              tinkerpopAttribs.clear();

              for (int i = 0, ilen = fields.size(); i < ilen; i++)
              {
                RecordField recordField = fields.get(i);

                String fieldName = recordField.getFieldName();

                Object fieldVal = record.getValue(recordField);
                sb.setLength(resetLen);
                sb.append(fieldName);
                tinkerpopAttribs.put(sb.toString(), fieldVal);
              }

              // enables us to override any...
              tinkerpopAttribs.putAll(tinkerpopFlowFileAttribs);

              ResultSet resSet = client.submit(queryStr, tinkerpopAttribs);
              CompletableFuture<List<Result>> results = resSet.all();

              if (results.isCompletedExceptionally())
              {
                results.exceptionally((Throwable throwable) -> {
                  getLogger().error("Server Error " + throwable.getMessage() + " orig msg: "+ resSet.getOriginalRequestMessage().toString());
                  //                                    session.transfer(tempFlowFile, REL_FAILURE);

                  throw new ProcessException(throwable);
                }).join();

              }
              List<Result> allRes = resSet.all().get();

              reqUUIDs.add(resSet.getOriginalRequestMessage().toString());

              //                            CompletableFuture<ResultSet> res = client.submitAsync(queryStr, tinkerpopAttribs);
              //                            resSets.add(res);

              recordCount.incrementAndGet();

            }

                        /*

//                        int i = 0;
                        for (CompletableFuture<ResultSet> compResSet : resSets) {
                            ResultSet resSet = compResSet.join();

                            CompletableFuture<List<Result>> results  = resSet.all();

                            if (results.isCompletedExceptionally()) {
                                results.exceptionally((Throwable throwable) -> {
                                    getLogger().error("Server Error " +
                                            resSet.getOriginalRequestMessage().toString(), throwable);
//                                    session.transfer(tempFlowFile, REL_FAILURE);

                                    throw new ProcessException(throwable);
                                }).join();

                            }
                            reqUUIDs.add(resSet.getOriginalRequestMessage().toString());


                        }
*/

            //                    } catch (final SchemaNotFoundException | MalformedRecordException | InterruptedException | ExecutionException |Throwable e) {
          }
          catch (final CompletionException | ProcessException pe)
          {
            throw pe;
          }
          catch (final Throwable e)
          {
            throw new ProcessException("Could not process incoming data", e);
          }
        }
      });
      attributes.put("reqUUIDs", reqUUIDs.toString());
      attributes.put("processed.record.count", String.valueOf(reqUUIDs.size()));
      attributes.put("requested.record.count", String.valueOf(recordCount.get()));

      FlowFile localFlowFile = original;
      localFlowFile = session.putAllAttributes(localFlowFile, attributes);
      session.transfer(localFlowFile, REL_SUCCESS);

    }
    catch (final Exception e)
    {
      handleError(e,flowFile,session,context);
      return;
    }

    final int count = recordCount.get();
    session.adjustCounter("Records Requested", count, false);
    session.adjustCounter("Records Processed", reqUUIDs.size(), false);
    getLogger().info("Successfully processed {} / records for {}", new Object[] { count, flowFile });
  }

}
