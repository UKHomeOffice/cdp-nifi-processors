/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import com.jayway.jsonpath.DocumentContext;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSObject;
import com.nimbusds.jose.JWSVerifier;
import com.pontusnetworks.utils.StringReplacer;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import java.security.Key;
import java.util.*;

/**
 * @author Leo Martins
 */
@Tags({ "pontus", "JWT", "Decoding",
    "JSON Web Tokens" }) @CapabilityDescription("Decode JWT Tokens.") public class JWTDecoderProcessor
    extends JWTCreatorProcessor
{

  public static final PropertyDescriptor JWT_VAL_OFFSET = new PropertyDescriptor.Builder().name("JWT Value Offset")
      .description("The offset to skip from either the file or attribute that has the JWT info to decode. ")
      .defaultValue("0").required(true).expressionLanguageSupported(false)
      .addValidator(StandardValidators.INTEGER_VALIDATOR).build();

  Integer jwtValueOffstet = null;

  @Override public void init(final ProcessorInitializationContext context)
  {
    List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(SSL_CONTEXT);
    properties.add(JWT_KEY_ALGO);
    properties.add(JWT_KEY_ALIAS);
    properties.add(JWT_ATTRIBUTE_NAME);
    properties.add(JWT_VAL_OFFSET);

    this.properties = Collections.unmodifiableList(properties);

    Set<Relationship> relationships = new HashSet<>();
    relationships.add(FAILURE);
    relationships.add(SUCCESS);
    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {

    super.onPropertyModified(descriptor, oldValue, newValue);

    if (descriptor.equals(JWT_VAL_OFFSET))
    {
      jwtValueOffstet = null;
    }
  }

  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {
    final ComponentLog log = this.getLogger();
    FlowFile flowfile = session.get();

    if (flowfile == null)
    {
      session.transfer(session.create(), FAILURE);
      return;

    }

    DocumentContext docCtx = null;

    if (sslService == null)
    {
      PropertyValue numBatchedEntriesPropVal = context.getProperty(SSL_CONTEXT);
      sslService = numBatchedEntriesPropVal.asControllerService(SSLContextService.class);

    }

    try
    {
      if (keyAlias == null)
      {
        keyAlias = context.getProperty(JWT_KEY_ALIAS).getValue();
      }
      if (signingKey == null)
      {
        signingKey = getPublicKey(sslService, keyAlias);
      }

      if (keyAlgo == null)
      {
        keyAlgo = JWSAlgorithm.parse(context.getProperty(JWT_KEY_ALGO).getValue());
      }
      if (jwtAttributeName == null)
      {
        jwtAttributeName = context.getProperty(JWT_ATTRIBUTE_NAME).getValue();

      }

      if (jwtValueOffstet == null)
      {
        jwtValueOffstet = Integer.parseInt(context.getProperty(JWT_VAL_OFFSET).getValue());

      }

      String jwtStr = null;

      boolean useAttribute = !(jwtAttributeName == null || jwtAttributeName.length() == 0);

      if (!useAttribute)
      {
        jwtStr = StringReplacer.readString(session, flowfile);
      }
      else
      {
        jwtStr = flowfile.getAttribute(jwtAttributeName);

      }

      Key key = getPublicKey(sslService, keyAlias);

      //      JWSSigner signer = getSigner(keyAlgo,key);

      //To parse the JWS and verify it, e.g. on client-side
      JWSObject jwsObject = JWSObject.parse(jwtStr.substring(jwtValueOffstet));

      JWSVerifier verifier = getVerifier(keyAlgo, key);

      if (!jwsObject.verify(verifier))
      {
        log.error("Failed to verify the JWT with the supplied key.");
        session.transfer(flowfile, FAILURE);
        return;

      }
      if (useAttribute)
      {
        flowfile = writeDataToAttrib(flowfile, session, jwtAttributeName, jwsObject.getPayload().toString());
      }
      else
      {
        flowfile = writeDataToFile(flowfile, session, jwsObject.getPayload().toString());
      }

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

}
