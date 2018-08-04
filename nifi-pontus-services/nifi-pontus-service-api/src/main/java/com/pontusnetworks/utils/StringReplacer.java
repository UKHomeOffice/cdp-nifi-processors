package com.pontusnetworks.utils;
/*
Copyright Leo Martins -
Pontus Vision - 2010-2017
*/

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//@Tags({ "pontus", "StringReplacer"})
//@CapabilityDescription("Fast String pattern replacement library.")
public class StringReplacer
{
  static Pattern regexPattern = Pattern.compile(Pattern.quote("~~{") + ".*?" + Pattern.quote("}~~"), Pattern.DOTALL);
  static Pattern numberRegexPattern = Pattern.compile("(\\d*)");

  //  public static final Configuration STRICT_PROVIDER_CONFIGURATION = Configuration.builder().jsonProvider(new JacksonJsonProvider()).build();
  //  public static final JsonProvider JSON_PROVIDER = STRICT_PROVIDER_CONFIGURATION.jsonProvider();
  private static String cronRegex = null;

  public static String greatestCommonPrefix(String a, String b)
  {
    if (a == null && b == null)
    {
      return "";
    }

    if (a.equals(b))
    {
      return a;
    }
    int minLength = Math.min(a.length(), b.length());
    for (int i = 0; i < minLength; i++)
    {
      if (a.charAt(i) != b.charAt(i))
      {
        return a.substring(0, i);
      }
    }
    return a.substring(0, minLength);
  }

  public static String replaceFirst(final String str, final String searchChars, String replaceChars)
  {
    if ("".equals(str) || "".equals(searchChars) || searchChars.equals(replaceChars))
    {
      return str;
    }
    if (replaceChars == null)
    {
      replaceChars = "";
    }
    final int strLength = str.length();
    final int searchCharsLength = searchChars.length();
    StringBuilder buf = new StringBuilder(str);
    boolean modified = false;
    int start = 0;
    for (int i = 0; i < strLength; i++)
    {
      start = buf.indexOf(searchChars, i);

      if (start == -1)
      {
        if (i == 0)
        {
          return str;
        }
        return buf.toString();
      }
      buf = buf.replace(start, start + searchCharsLength, replaceChars);
      modified = true;
      return buf.toString();

    }
    if (!modified)
    {
      return str;
    }
    else
    {
      return buf.toString();
    }
  }

  public static String replaceAll(final String str, final String searchChars, String replaceChars)
  {
    if ("".equals(str) || "".equals(searchChars) || searchChars.equals(replaceChars))
    {
      return str;
    }
    if (replaceChars == null)
    {
      replaceChars = "";
    }
    final int strLength = str.length();
    final int searchCharsLength = searchChars.length();
    StringBuilder buf = new StringBuilder(str);
    boolean modified = false;
    int start = 0;
    for (int i = 0; i < strLength; i++)
    {
      start = buf.indexOf(searchChars, i);

      if (start == -1)
      {
        if (i == 0)
        {
          return str;
        }
        return buf.toString();
      }
      buf = buf.replace(start, start + searchCharsLength, replaceChars);
      modified = true;

    }
    if (!modified)
    {
      return str;
    }
    else
    {
      return buf.toString();
    }
  }

  public static String replaceTokensEscapeSingleQuotes(Map<String, String> nameValuePairs, String templateStr,
                                                       int maxReplacements, String escapeChar)
  {
    if (nameValuePairs == null || templateStr == null)
    {
      return null;
    }
    int counter = 0;
    final StringBuilder sb = new StringBuilder("~~{");
    String lastName = null;
    final String escapedSingleQuote = escapeChar + "'";
    try
    {

      while ((templateStr.contains("~~{")) && counter < maxReplacements /*
       * failsafe
       * stop
       * to
       * prevent
       * infinite
       * recursion
       */)
      {
        for (String name : nameValuePairs.keySet())
        {
          // Pattern p = Pattern.compile(Pattern.quote(name));
          // templateStr =
          // p.matcher(templateStr).replaceAll(nameValuePairs.get(name));
          sb.setLength(3); // sizeof "~~{"
          lastName = name;
          sb.append(lastName);
          sb.append("}~~");

          final String val = replaceFirst(nameValuePairs.get(name), "'", escapedSingleQuote);
          templateStr = replaceAll(templateStr, sb.toString(), val);

          //
          // templateStr = org.apache.commons.lang3.StringUtils.replaceChars(
          // templateStr,
          // name,
          // nameValuePairs.get(name));
          // templateStr = templateStr.replaceAll(Pattern.quote(name),
          // nameValuePairs.get(name));

        }
        counter++;
      }
    }
    catch (Exception e)
    {
      //            LOGGER.info(new StringBuffer("Failed to replace all patterns <~~{").append(lastName).append("}~~> with <")
      //                    .append(nameValuePairs.get(lastName)).append(">; trying simple replace.").toString());
      e.printStackTrace();
    }
    return templateStr;

  }

  public static Map<String, JsonPath> parseJSONPaths(String templateStr)
  {
    int lastTokenIndex = 0;
    int templateLen = templateStr.length();
    StringBuilder sb = new StringBuilder("~~{");
    String lastName = null;
    Map<String, JsonPath> retVal = new HashMap<>();

    do
    {
      int startOfToken = templateStr.indexOf("~~{", lastTokenIndex);
      int endOfToken = templateStr.indexOf("}~~", startOfToken);
      String name = templateStr.substring(startOfToken + 3, endOfToken);

      lastTokenIndex = endOfToken + 3;
      if (endOfToken == -1 || startOfToken == -1)
      {
        break;
      }

      sb.setLength(3); // sizeof "~~{"
      lastName = name;
      sb.append(lastName);
      sb.append("}~~");

      JsonPath jp = JsonPath.compile(name);

      retVal.put(name, jp);

    } while (lastTokenIndex < templateLen);

    return retVal;
  }

  public static String replaceTokens(Map<String, JsonPath> nameValuePairs, JsonNode jsonNode, String templateStr,
                                     int maxReplacements)
  {
    if (nameValuePairs == null || templateStr == null)
    {
      return null;
    }
    DocumentContext docCtx = JsonPath.parse(jsonNode.toString());

    int counter = 0;
    StringBuilder sb = new StringBuilder("~~{");
    String lastName = null;
    try
    {

      while ((templateStr.contains("~~{")) && counter < maxReplacements /*
       * failsafe
       * stop
       * to
       * prevent
       * infinite
       * recursion
       */)
      {
        for (String name : nameValuePairs.keySet())
        {
          // Pattern p = Pattern.compile(Pattern.quote(name));
          // templateStr =
          // p.matcher(templateStr).replaceAll(nameValuePairs.get(name));
          sb.setLength(3); // sizeof "~~{"
          lastName = name;
          sb.append(lastName);
          sb.append("}~~");

          JsonPath jp = nameValuePairs.get(name);
          try
          {
            String val = docCtx.read(jp);
            templateStr = replaceAll(templateStr, sb.toString(), val);
          }
          catch (Exception e)
          {
            // ignore...
          }
          //
          // templateStr = org.apache.commons.lang3.StringUtils.replaceChars(
          // templateStr,
          // name,
          // nameValuePairs.get(name));
          // templateStr = templateStr.replaceAll(Pattern.quote(name),
          // nameValuePairs.get(name));

        }
        counter++;
      }
    }
    catch (Exception e)
    {
      //            LOGGER.info(new StringBuffer("Failed to replace all patterns <~~{").append(lastName).append("}~~> with <")
      //                    .append(nameValuePairs.get(lastName)).append(">; trying simple replace.").toString());

    }
    return templateStr;
  }

  public static JsonNode readJson(ProcessSession processSession, FlowFile flowFile)
  {
    JsonNode rootNode = null; // or URL, Stream, Reader, String, byte[]

    // Parse the document once into an associated context to support multiple path evaluations if specified
    final AtomicReference<JsonNode> contextHolder = new AtomicReference<>(null);
    processSession.read(flowFile, new InputStreamCallback()
    {
      @Override public void process(InputStream in) throws IOException
      {
        BufferedInputStream bufferedInputStream = new BufferedInputStream(in);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode rootNode = mapper.readValue(bufferedInputStream, JsonNode.class);
        contextHolder.set(rootNode);
      }
    });

    return contextHolder.get();
  }

  public static String readString(ProcessSession processSession, FlowFile flowFile)
  {

    final StringBuffer strBuf = new StringBuffer();

    // Parse the document once into an associated context to support multiple path evaluations if specified
    processSession.read(flowFile, new InputStreamCallback()
    {
      @Override public void process(InputStream in) throws IOException
      {

        BufferedInputStream bufferedInputStream = new BufferedInputStream(in);
        byte[] contents = new byte[1024];

        int bytesRead = 0;
        String strFileContents;
        while ((bytesRead = in.read(contents)) != -1)
        {
          strBuf.append(new String(contents, 0, bytesRead));
        }
      }
    });

    return strBuf.toString();
  }

  public static String replaceTokens(Map<String, String> nameValuePairs, String templateStr, int maxReplacements)
  {
    if (nameValuePairs == null || templateStr == null)
    {
      return null;
    }
    int counter = 0;
    StringBuilder sb = new StringBuilder("~~{");
    String lastName = null;
    try
    {

      while ((templateStr.contains("~~{")) && counter < maxReplacements /*
       * failsafe
       * stop
       * to
       * prevent
       * infinite
       * recursion
       */)
      {
        for (String name : nameValuePairs.keySet())
        {
          // Pattern p = Pattern.compile(Pattern.quote(name));
          // templateStr =
          // p.matcher(templateStr).replaceAll(nameValuePairs.get(name));
          sb.setLength(3); // sizeof "~~{"
          lastName = name;
          sb.append(lastName);
          sb.append("}~~");

          templateStr = replaceAll(templateStr, sb.toString(), nameValuePairs.get(name));

          //
          // templateStr = org.apache.commons.lang3.StringUtils.replaceChars(
          // templateStr,
          // name,
          // nameValuePairs.get(name));
          // templateStr = templateStr.replaceAll(Pattern.quote(name),
          // nameValuePairs.get(name));

        }
        counter++;
      }
    }
    catch (Exception e)
    {
      //            LOGGER.info(new StringBuffer("Failed to replace all patterns <~~{").append(lastName).append("}~~> with <")
      //                    .append(nameValuePairs.get(lastName)).append(">; trying simple replace.").toString());

    }
    return templateStr;
  }

  public static String replaceAddOne(String defaultVal)
  {
    String retVal = defaultVal;
    try
    {
      Matcher matcher = numberRegexPattern.matcher(defaultVal);
      String lastMatch = "";
      while (matcher.find())
      {
        String expressionStrRaw = matcher.group();
        if (!expressionStrRaw.isEmpty())
        {
          lastMatch = expressionStrRaw;
        }
      }

      if (!lastMatch.isEmpty())
      {
        Integer num = Integer.valueOf(lastMatch);

        num++;

        String tempNum = num.toString();
        int numDigits = lastMatch.length();

        String tempRetVal = tempNum;
        if (tempNum.length() < numDigits)
        {
          tempRetVal = String.format("%0" + numDigits + "d", num);
        }
        StringBuilder strBuilder = new StringBuilder(retVal);
        strBuilder
            .replace(retVal.lastIndexOf(lastMatch), retVal.lastIndexOf(lastMatch) + lastMatch.length(), tempRetVal);
        return strBuilder.toString();

        // return retVal.replace(lastMatch, tempRetVal);

      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      // ignore.
    }
    return retVal;
  }

  public static String getCronRegex()
  {
    if (cronRegex == null)
    {
      // numbers intervals and regex
      Map<String, String> numbers = new HashMap<String, String>();
      numbers.put("sec", "[0-5]?\\d");
      numbers.put("min", "[0-5]?\\d");
      numbers.put("hour", "[01]?\\d|2[0-3]");
      numbers.put("day", "0?[1-9]|[12]\\d|3[01]");
      numbers.put("month", "[1-9]|1[012]");
      numbers.put("dow", "[0-6]");
      numbers.put("year", "|\\d{4}");

      Map<String, String> field_re = new HashMap<String, String>();

      // expand regex to contain different time specifiers
      for (String field : numbers.keySet())
      {
        String number = numbers.get(field);
        String range = "(?:" + number + ")(?:(?:-|\\/|\\," + ("dow".equals(field) ? "|#" : "") +

            ")(?:" + number + "))?" + ("dow".equals(field) ? "(?:L)?" : ("month".equals(field) ? "(?:L|W)?" : ""));
        field_re.put(field, "\\?|\\*|" + range + "(?:," + range + ")*");
      }

      // add string specifiers
      String monthRE = field_re.get("month");
      String monthREVal = "JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC";
      String monthRERange = "(?:" + monthREVal + ")(?:(?:-)(?:" + monthREVal + "))?";
      monthRE = monthRE + "|\\?|\\*|" + monthRERange + "(?:," + monthRERange + ")*";
      field_re.put("month", monthRE);

      String dowRE = field_re.get("dow");
      String dowREVal = "MON|TUE|WED|THU|FRI|SAT|SUN";
      String dowRERange = "(?:" + dowREVal + ")(?:(?:-)(?:" + dowREVal + "))?";

      dowRE = dowRE + "|\\?|\\*|" + dowRERange + "(?:," + dowRERange + ")*";
      field_re.put("dow", dowRE);

      StringBuilder fieldsReSB = new StringBuilder();
      fieldsReSB.append("^\\s*(").append("$") //
          .append("|#") //
          .append("|\\w+\\s*=") //
          .append("|") //
          .append("(")//
          .append(field_re.get("sec")).append(")\\s+(")//
          .append(field_re.get("min")).append(")\\s+(")//
          .append(field_re.get("hour")).append(")\\s+(")//
          .append(field_re.get("day")).append(")\\s+(")//
          .append(field_re.get("month")).append(")\\s+(")//
          .append(field_re.get("dow")).append(")(|\\s)+(")//
          .append(field_re.get("year"))//
          .append(")")//
          .append(")")//
          .append("$");

      cronRegex = fieldsReSB.toString();
    }
    return cronRegex;
  }

  public static String convertCPUListToHex(final String csvCPUList, final int numHexBytes, final int leadingZeros,
                                           final int sizeOfLong)
  {
    String[] bits = csvCPUList.split(",");

    long[] data = new long[(int) Math.floor(numHexBytes / sizeOfLong) + (((numHexBytes % sizeOfLong) == 0) ? 0 : 1)];
    for (String bit : bits)
    {
      if (bit.contains("-"))
      {
        String[] bitRange = bit.split("-");

        for (int i = Integer.valueOf(bitRange[0]), size = Integer.valueOf(bitRange[1]); i <= size; i++)
        {
          setDataMask(i, data, sizeOfLong);
        }
      }
      else
      {
        int bitNum = Integer.valueOf(bit.trim());
        setDataMask(bitNum, data, sizeOfLong);
      }

    }
    StringBuffer retVal = new StringBuffer();

    for (int i = data.length - 1; i >= 0; i--)
    {
      long val = data[i];
      String tempVal = Long.toHexString(val);
      for (int leadingZerosIndex = tempVal.length(); leadingZerosIndex < leadingZeros; leadingZerosIndex++)
      {
        retVal.append("0");
      }
      retVal.append(tempVal);

      if (i != 0)
      {
        retVal.append(",");
      }
    }

    return retVal.toString();

  }

  private static void setDataMask(final int bitNum, final long[] data, final int sizeOfLongInBytes)
  {

    final int dataIndex = (int) (bitNum / (sizeOfLongInBytes * 8));

    if (dataIndex >= data.length)
    {
      // truncate if we don't have enough hex bytes.
      return;
    }

    int lastBitNum = bitNum;
    for (int i = 0; i < dataIndex; i++)
    {
      lastBitNum -= (sizeOfLongInBytes * 8);
    }
    long mask = 0x01L;
    mask = mask << lastBitNum;
    data[dataIndex] |= mask;
  }
}
