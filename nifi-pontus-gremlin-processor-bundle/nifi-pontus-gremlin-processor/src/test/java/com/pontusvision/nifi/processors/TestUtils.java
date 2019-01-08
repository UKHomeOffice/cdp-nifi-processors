package com.pontusvision.nifi.processors;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

public class TestUtils
{
  public static byte[] getFileBytes(final String filePath) throws IOException
  {
    return getFileContent(filePath).getBytes();
  }

  public static String getFileContent(final String filePath) throws IOException {
    return IOUtils.toString(getFileInputStream(filePath), "UTF-8");
  }

  public static InputStream getFileInputStream(final String filePath) {
    InputStream is = TestUtils.class.getClassLoader().getResourceAsStream(filePath);
    assert is != null : "File does not exist - " + filePath;
    return is;
  }

}
