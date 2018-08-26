package com.pontusvision.nifi.processors;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import javax.script.Bindings;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

//import java.util.*;


@EventDriven  @SupportsBatching @InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
//@SideEffectFree
@Tags({ "GDPR", "record","bulkload", "Write", "schema", "tinkerpop", "gremlin", "json", "csv", "avro", "log", "logs", "freeform",
    "text" }) @WritesAttributes({
    @WritesAttribute(attribute = "processed.record.count", description = "The number of records processed"),
    @WritesAttribute(attribute = "requested.record.count", description = "The number of records processed") }) @CapabilityDescription(
    "Reads data from a Record Reader Controller Service, and puts it IN BULK into a TinkerPop Gremlin Server using a given query.  "
        + "Each of the schema fields is passed inside a list of maps, with one map per record to the tinkerpop query string with the variable name listOfMaps.  "
        + "As an example, if the records came in as [{ foo:123, bar: 'hello' }, {foo:453, bar:'hello2}, these would appear as a list of 2 maps:\n"
        + "   /*List<Map<String,String>> listOfMaps*/\n"
        + "   listOfMaps.get(0).get(\"foo\") ; /* = 123 */ \n"
        + "   listOfMaps.get(0).get(\"bar\") ; /* = hello */ \n"
        + "   listOfMaps.get(1).get(\"foo\") ; /* = 453 */ \n"
        + "   listOfMaps.get(1).get(\"bar\") ; /* = hello2 */ \n"
        + ""
        + "")

public class PontusTinkerPopClientRecordBulk extends PontusTinkerPopClientRecord
{

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

    final Map<String, String> tinkerpopFlowFileAttribs = allAttribs.entrySet().stream()
        .filter((entry -> entry.getKey().startsWith(queryAttribPrefixStr)))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    final FlowFile original = flowFile;
    final Map<String, String> originalAttributes = flowFile.getAttributes();
    attributes.putAll(originalAttributes);

    try
    {
      final FlowFile tempFlowFile = flowFile;

      flowFile = session.write(flowFile, (in, out) -> {

        try (final RecordReader reader = readerFactory.createRecordReader(original, in, getLogger()))
        {

          final RecordSchema rSchema = reader.getSchema();

          List<RecordField> fields = rSchema.getFields();

          Record record;

          List<CompletableFuture<ResultSet>> resSets = new LinkedList<>();
          int resetLen = queryAttribPrefixStr.length();
          StringBuilder sb = new StringBuilder(queryAttribPrefixStr);
          List<Map<String, String>> bulkLoad = new ArrayList<>();

          int count = 0;
          while ((record = reader.nextRecord()) != null)
          {
            Map<String, String> tinkerpopAttribs = new HashMap<>(fields.size() + tinkerpopFlowFileAttribs.size());

            for (int i = 0, ilen = fields.size(); i < ilen; i++)
            {
              RecordField recordField = fields.get(i);

              String fieldName = recordField.getFieldName();

              Object fieldVal = record.getValue(recordField);
              sb.setLength(resetLen);
              sb.append(fieldName);
              tinkerpopAttribs.put(sb.toString(), fieldVal != null? fieldVal.toString(): null);
            }

            // override any record attribs from the GUI.
            tinkerpopAttribs.putAll(tinkerpopFlowFileAttribs);
            bulkLoad.add(tinkerpopAttribs);
            count++;

          }

          Map<String, Object> bulkLoadAttr = new HashMap<>();
          bulkLoadAttr.put("listOfMaps", bulkLoad);

          Bindings bindings = getBindings(tempFlowFile);

          bindings.putAll(bulkLoadAttr);

          String queryString = getQueryStr(session);

          byte[] res = runQuery(bindings, queryString);
          FlowFile localFlowFile = original;
          localFlowFile = session.putAllAttributes(localFlowFile, attributes);
          localFlowFile = session.write(localFlowFile, out2 -> out2.write(res));

          session.transfer(localFlowFile, REL_SUCCESS);

          recordCount.set(count);


        }
        catch (final CompletionException | ProcessException pe)
        {
          throw pe;
        }
        catch (final Throwable e)
        {
          throw new ProcessException("Could not process incoming data", e);
        }
      });

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
