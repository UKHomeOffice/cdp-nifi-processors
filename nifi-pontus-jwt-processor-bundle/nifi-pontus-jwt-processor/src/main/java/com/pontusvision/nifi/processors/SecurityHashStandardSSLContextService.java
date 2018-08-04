package com.pontusvision.nifi.processors;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by leo on 20/03/2017.
 */

@Tags({ "pontus", "ssl", "secure", "certificate", "keystore", "truststore", "jceks", "jks", "p12", "pkcs12",
    "pkcs" }) @CapabilityDescription(
    "pontus implementation of the SSLContextService. Provides the ability to configure "
        + "keystore and/or truststore properties once and reuse that configuration throughout the application")

public class SecurityHashStandardSSLContextService extends AbstractControllerService implements SSLContextService
{

  public static final String STORE_TYPE_JKS = "JKS";
  public static final String STORE_TYPE_JCEKS = "JCEKS";
  public static final String STORE_TYPE_PKCS12 = "PKCS12";
  public static final PropertyDescriptor KEYSTORE_TYPE = new PropertyDescriptor.Builder().name("Keystore Type")
      .description("The Type of the Keystore. JKS, JCEKS or PKCS12")
      .allowableValues(STORE_TYPE_JKS, STORE_TYPE_PKCS12, STORE_TYPE_JCEKS)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).sensitive(false).build();

  public static final PropertyDescriptor TRUSTSTORE_TYPE = new PropertyDescriptor.Builder().name("Truststore Type")
      .description("The Type of the Truststore. JKS, JCEKS or PKCS12")
      .allowableValues(STORE_TYPE_JKS, STORE_TYPE_PKCS12, STORE_TYPE_JCEKS)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).sensitive(false).build();

  public static final PropertyDescriptor TRUSTSTORE = new PropertyDescriptor.Builder().name("Truststore Filename")
      .description("The fully-qualified filename of the Truststore").defaultValue(null)
      .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR).sensitive(false).build();

  public static final PropertyDescriptor TRUSTSTORE_PASSWORD = new PropertyDescriptor.Builder()
      .name("Truststore Password").description("The password for the Truststore").defaultValue(null)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).sensitive(true).build();
  public static final PropertyDescriptor KEYSTORE = new PropertyDescriptor.Builder().name("Keystore Filename")
      .description("The fully-qualified filename of the Keystore").defaultValue(null)
      .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR).sensitive(false).build();
  public static final PropertyDescriptor KEYSTORE_PASSWORD = new PropertyDescriptor.Builder().name("Keystore Password")
      .defaultValue(null).description("The password for the Keystore")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).sensitive(true).build();
  public static final PropertyDescriptor KEY_PASSWORD = new PropertyDescriptor.Builder().name("key-password")
      .displayName("Key Password").description(
          "The password for the key. If this is not specified, but the Keystore Filename, Password, and Type are specified, "
              + "then the Keystore Password will be assumed to be the same as the Key Password.")
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).sensitive(true).required(false).build();

  private static final List<PropertyDescriptor> properties;

  static
  {
    List<PropertyDescriptor> props = new ArrayList<>();
    props.add(KEYSTORE);
    props.add(KEYSTORE_PASSWORD);
    props.add(KEY_PASSWORD);
    props.add(KEYSTORE_TYPE);
    props.add(TRUSTSTORE);
    props.add(TRUSTSTORE_PASSWORD);
    props.add(TRUSTSTORE_TYPE);
    properties = Collections.unmodifiableList(props);
  }

  private ConfigurationContext configContext;

  @OnEnabled public void onConfigured(final ConfigurationContext context) throws InitializationException
  {
    configContext = context;

  }

  @Override protected List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    return properties;
  }

  @Override public SSLContext createSSLContext(ClientAuth clientAuth) throws ProcessException
  {
    return null;
  }

  public String getTrustStoreFile()
  {
    return configContext.getProperty(TRUSTSTORE).getValue();
  }

  public String getTrustStoreType()
  {
    return configContext.getProperty(TRUSTSTORE_TYPE).getValue();
  }

  public String getTrustStorePassword()
  {
    return configContext.getProperty(TRUSTSTORE_PASSWORD).getValue();
  }

  public boolean isTrustStoreConfigured()
  {
    return getTrustStoreFile() != null && getTrustStorePassword() != null && getTrustStoreType() != null;
  }

  public String getKeyStoreFile()
  {
    return configContext.getProperty(KEYSTORE).getValue();
  }

  public String getKeyStoreType()
  {
    return configContext.getProperty(KEYSTORE_TYPE).getValue();
  }

  public String getKeyStorePassword()
  {
    return configContext.getProperty(KEYSTORE_PASSWORD).getValue();
  }

  public String getKeyPassword()
  {
    return configContext.getProperty(KEY_PASSWORD).getValue();
  }

  public boolean isKeyStoreConfigured()
  {
    return getKeyStoreFile() != null && getKeyStorePassword() != null && getKeyStoreType() != null;
  }

  @Override public String getSslAlgorithm()
  {
    return null;
  }

  @Override public String toString()
  {
    return "SSLContextService[id=" + getIdentifier() + "]";
  }
}
