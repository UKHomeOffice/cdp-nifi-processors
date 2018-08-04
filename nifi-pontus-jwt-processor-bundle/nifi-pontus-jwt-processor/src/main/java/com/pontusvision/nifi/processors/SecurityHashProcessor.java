/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import javax.crypto.Mac;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.Key;
import java.security.KeyStore;
import java.util.*;

//import io.jsonwebtoken.Jwts;
//import io.jsonwebtoken.SignatureAlgorithm;

/**
 * @author Leo Martins
 */
@Tags({ "pontus", "Hash",
    "Security" }) @CapabilityDescription("Creates a hash from a string using a secret key.") public class SecurityHashProcessor
    extends AbstractProcessor
{

  public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
      .name("Security Controller Service")
      .description("The Security Context Service to use when creating secure hashes").required(true)
      .identifiesControllerService(SSLContextService.class).build();
  public static final PropertyDescriptor HASH_ENCRYPTED_STRING_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
      .name("Encrypted String Hash Attribute Name")
      .description("The name of the attribute where this processor write the encrypted hash")
      .defaultValue("hashed_string_output").required(true).expressionLanguageSupported(false)
      .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR).build();
  public static final PropertyDescriptor HASH_RAW_STRING = new PropertyDescriptor.Builder()
      .name("Input Raw String pre-hash").description("The string to be hashed").defaultValue("").required(true)
      .expressionLanguageSupported(true).addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
      .build();
  public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
      .description("Success relationship").build();

  //  public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder()
  //    .name("Security Controller Service")
  //    .description("The Security Context Service to use when creating secure hashes")
  //    .required(true)
  //    .identifiesControllerService(SecurityHashStandardSSLContextService.class)
  //    .build();
  public static final Relationship FAILURE = new Relationship.Builder().name("FAILURE")
      .description("Failure relationship").build();
  final static Validator keySignValidator = new Validator()
  {
    @Override public ValidationResult validate(String subject, String input, ValidationContext context)
    {
      ValidationResult.Builder builder = new ValidationResult.Builder();

      boolean isValid = false;

      String message = "Incompatible values for TLS Key, alias, and sign algorithm:";
      try
      {

        PropertyValue ctx = context.getProperty(SSL_CONTEXT);
        SSLContextService sslService = ctx.asControllerService(SSLContextService.class);

        String algoStr = context.getProperty(HASH_KEY_ALGO).getValue();
        String alias = context.getProperty(HASH_KEY_ALIAS).getValue();
        encodeHash(getKey(sslService, alias), algoStr, "HELLO WORLD");
        isValid = true;

      }
      catch (Exception e)
      {
        message += e.getLocalizedMessage();
      }

      builder.valid(isValid);
      builder.input(input);
      builder.subject(subject);

      if (!isValid)
      {
        builder.explanation(message);
      }

      ValidationResult res = builder.build();

      return res;
    }
  };
  public static final PropertyDescriptor HASH_KEY_ALIAS = new PropertyDescriptor.Builder()
      .name("Security Context Key Alias")
      .description("The Alias for the key in the key store of the SSL context passed through the option above. ")
      .defaultValue("jwthashkey").required(true).expressionLanguageSupported(false).addValidator(keySignValidator)
      .build();
  public static final PropertyDescriptor HASH_KEY_ALGO = new PropertyDescriptor.Builder().name("Hash Key Algo")
      .description(
          "The encryption keyAlgo for the key in the key store of the TLS context passed through the option above. ")
      .defaultValue("HmacSHA256").required(true).expressionLanguageSupported(false).addValidator(keySignValidator)
      .allowableValues("HmacSHA1", "HmacSHA256", "HmacSHA384", "HmacSHA512").build();
  protected List<PropertyDescriptor> properties;
  protected Set<Relationship> relationships;
  SSLContextService sslService = null;
  Key signingKey = null;
  String keyAlgo = "";
  String keyAlias = null;

  String hashEncryptedStrAttributeName = "";

  public static Key getKey(SSLContextService sslService, String alias) throws Exception
  {
    FileInputStream is = new FileInputStream(sslService.getKeyStoreFile());

    KeyStore keystore = KeyStore.getInstance(sslService.getKeyStoreType());
    keystore.load(is, sslService.getKeyStorePassword().toCharArray());

    Key key = keystore.getKey(alias, sslService.getKeyPassword().toCharArray());

    return key;
  }

  public static String encodeHash(Key key, String algoStr, String message) throws Exception
  {

    Mac mac = Mac.getInstance(algoStr);
    mac.init(key);
    byte[] rawHmac = mac.doFinal(message.getBytes());
    return Base64.getEncoder().encodeToString(rawHmac);
  }

  public static FlowFile writeDataToAttrib(FlowFile flowFile, ProcessSession session, final String jwtAttributeName,
                                           final String data)
  {
    flowFile = session.putAttribute(flowFile, jwtAttributeName, data);
    return flowFile;
  }

  public static FlowFile writeDataToFile(FlowFile flowfile, ProcessSession session, final String data)
  {

    flowfile = session.write(flowfile, new OutputStreamCallback()
    {

      @Override public void process(OutputStream out) throws IOException
      {
        out.write(data.getBytes());
      }
    });

    return flowfile;
    //    session.transfer(flowfile, SUCCESS);

  }

  @Override public void init(final ProcessorInitializationContext context)
  {
    List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(SSL_CONTEXT);
    properties.add(HASH_KEY_ALGO);
    properties.add(HASH_KEY_ALIAS);
    properties.add(HASH_ENCRYPTED_STRING_ATTRIBUTE_NAME);
    properties.add(HASH_RAW_STRING);

    this.properties = Collections.unmodifiableList(properties);

    Set<Relationship> relationships = new HashSet<>();
    relationships.add(FAILURE);
    relationships.add(SUCCESS);
    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {
    if (descriptor.equals(SSL_CONTEXT) || descriptor.equals(HASH_KEY_ALIAS) || descriptor.equals(HASH_KEY_ALGO))
    {
      sslService = null;
      signingKey = null;
      keyAlias = null;
      keyAlgo = null;
    }

    if (descriptor.equals(HASH_ENCRYPTED_STRING_ATTRIBUTE_NAME))
    {
      hashEncryptedStrAttributeName = null;
    }

  }

  @Override

  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {
    final ComponentLog log = this.getLogger();
    FlowFile flowfile = session.get();

    if (flowfile == null)
    {
      session.transfer(flowfile, FAILURE);
      return;

    }

    if (hashEncryptedStrAttributeName == null)
    {
      hashEncryptedStrAttributeName = context.getProperty(HASH_ENCRYPTED_STRING_ATTRIBUTE_NAME).getValue();
    }

    if (sslService == null)
    {
      PropertyValue sslCtxProp = context.getProperty(SSL_CONTEXT);
      sslService = sslCtxProp.asControllerService(SSLContextService.class);
    }

    try
    {
      if (keyAlias == null)
      {
        keyAlias = context.getProperty(HASH_KEY_ALIAS).getValue();
      }
      if (signingKey == null)
      {
        signingKey = getKey(sslService, keyAlias);
      }
      if (keyAlgo == null)
      {
        keyAlgo = (context.getProperty(HASH_KEY_ALGO).getValue());
      }

      String rawStr = context.getProperty(HASH_RAW_STRING).evaluateAttributeExpressions(flowfile).getValue();

      String encodedStr = encodeHash(signingKey, keyAlgo, rawStr);

      flowfile = writeDataToAttrib(flowfile, session, hashEncryptedStrAttributeName, encodedStr);

    }
    catch (Exception e)
    {
      log.error("Failed to get signing key from sslService " + sslService.toString(), e);
      session.transfer(flowfile, FAILURE);
      return;

    }

    session.transfer(flowfile, SUCCESS);

    //    session.remove(flowfile);
    //    session.commit();

  }

  @Override public Set<Relationship> getRelationships()
  {
    return relationships;
  }

  @Override public List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    return properties;
  }

}
