/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import com.jayway.jsonpath.DocumentContext;
import com.nimbusds.jose.*;
import com.nimbusds.jose.crypto.*;
import com.nimbusds.jwt.JWTClaimsSet;
import com.pontusnetworks.utils.StringReplacer;
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

import javax.crypto.SecretKey;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.*;

//import io.jsonwebtoken.Jwts;
//import io.jsonwebtoken.SignatureAlgorithm;

/**
 * @author Leo Martins
 */
@Tags({ "pontus", "JWT", "Creation",
    "JSON Web Tokens" }) @CapabilityDescription("Create JWT Tokens.") public class JWTCreatorProcessor
    extends AbstractProcessor
{

  public static final PropertyDescriptor SSL_CONTEXT = new PropertyDescriptor.Builder().name("TLS Context Service")
      .description("The TLS Context Service to use in order to sign the JWTs created").required(true)
      .identifiesControllerService(SSLContextService.class).build();
  public static final PropertyDescriptor JWT_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
      .name("JWT Attribute Name").description(
          "If present, the attribute name where the JWT will be sent to (if creating), or read from (if decoding). ")
      .defaultValue("").required(true).expressionLanguageSupported(false).addValidator(new Validator()
      {
        @Override public ValidationResult validate(String subject, String input, ValidationContext context)
        {
          ValidationResult.Builder builder = new ValidationResult.Builder();

          builder.subject(subject).input(input).valid(true);

          return builder.build();
        }
      }).build();
  public static final PropertyDescriptor JWT_TIME_TO_LIVE_MS = new PropertyDescriptor.Builder().name("JWT time to live")
      .description("The JWT time to live in milliseconds; use -1 for unlimited. ").defaultValue("-1").required(true)
      .expressionLanguageSupported(false).addValidator(StandardValidators.INTEGER_VALIDATOR).build();
  public static final Relationship SUCCESS = new Relationship.Builder().name("SUCCESS")
      .description("Success relationship").build();
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

        PropertyValue numBatchedEntriesPropVal = context.getProperty(SSL_CONTEXT);
        SSLContextService sslService = numBatchedEntriesPropVal.asControllerService(SSLContextService.class);

        JWSAlgorithm algo = JWSAlgorithm.parse(context.getProperty(JWT_KEY_ALGO).getValue());
        String alias = context.getProperty(JWT_KEY_ALIAS).getValue();

        Payload payload = new Payload("Hello, world!");
        Key key = getPrivateKey(sslService, alias);
        JWSSigner signer = getSigner(algo, key);
        JWSObject jwsObject = new JWSObject(new JWSHeader(algo), payload);
        jwsObject.sign(signer);

        //
        ////        SignatureAlgorithm algo = SignatureAlgorithm.forName(context.getProperty(HASH_KEY_ALGO).getValue());
        //        String compactJws = Jwts.builder()
        //          .setSubject("test")
        //          .claim("iss", "org")
        //          .claim("bizCtx", "bizCtx")
        //          .signWith(algo, key)
        //          //  .signWith(SignatureAlgorithm.forName(sslService.getSslAlgorithm()), signingKey)
        //          .compact();

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
  public static final PropertyDescriptor JWT_KEY_ALGO = new PropertyDescriptor.Builder().name("JWT Key Algo")
      .description(
          "The encryption keyAlgo for the key in the key store of the TLS context passed through the option above. ")
      .defaultValue(JWSAlgorithm.RS512.toString()).required(true).expressionLanguageSupported(false)
      .addValidator(keySignValidator).allowableValues(getAlgos()).build();
  public static final PropertyDescriptor JWT_KEY_ALIAS = new PropertyDescriptor.Builder()
      .name("JWT TLS Context Key Alias")
      .description("The TLS Alias for key in the key store of the SSL context passed through the option above. ")
      .defaultValue("jwtkey").required(true).expressionLanguageSupported(false).addValidator(keySignValidator).build();

  //  public static final PropertyDescriptor JWT_SUBJECT_JSON_PATH = new PropertyDescriptor.Builder()
  //    .name("JWT Subject (User Name) JSON PATH")
  //    .description("The JSON Path for the JWT Subject. " +
  //      " JSON Path is a language to access json objects  (similar to what Xpath does for XML; e.g. $.foo will point to the \"foo\" member of a JSON object and get its value).")
  //    .defaultValue("$.sub")
  //    .required(true)
  //    .expressionLanguageSupported(false)
  //    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
  //    .build();
  //
  //  public static final PropertyDescriptor JWT_ORGANIZATION_JSON_PATH = new PropertyDescriptor.Builder()
  //    .name("JWT Organization JSON PATH")
  //    .description("The JSON Path for the JWT Organization. " +
  //      " JSON Path is a language to access json objects  (similar to what Xpath does for XML; e.g. $.foo will point to the \"foo\" member of a JSON object and get its value).")
  //    .defaultValue("$.iss")
  //    .required(true)
  //    .expressionLanguageSupported(false)
  //    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
  //    .build();
  //
  //  public static final PropertyDescriptor JWT_BIZ_CONTEXT_JSON_PATH = new PropertyDescriptor.Builder()
  //    .name("JWT Subject JSON PATH")
  //    .description("The JSON Path for the JWT Subject. " +
  //      " JSON Path is a language to access json objects  (similar to what Xpath does for XML; e.g. $.foo will point to the \"foo\" member of a JSON object and get its value).")
  //    .defaultValue("$.bizctx")
  //    .required(true)
  //    .expressionLanguageSupported(false)
  //    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
  //    .build();
  //
  protected List<PropertyDescriptor> properties;
  protected Set<Relationship> relationships;
  SSLContextService sslService = null;
  Key signingKey = null;
  JWSAlgorithm keyAlgo = JWSAlgorithm.RS512;
  String keyAlias = null;

  String subjJsonPathStr = "sub";
  String orgJsonPathStr = "iss";
  String bizCtxJsonPathStr = "bizctx";

  String jwtAttributeName = "";

  int jwtTTL = -2;

  //  JsonPath subjJsonPath = JsonPath.compile(subjJsonPathStr);
  //  JsonPath orgJsonPath = JsonPath.compile(orgJsonPathStr);
  //  JsonPath bizCtxJsonPath = JsonPath.compile(bizCtxJsonPathStr);

  //  public static String[] getNames(Class<? extends Enum<?>> e) {
  //    return Arrays.stream(e.getEnumConstants()).map(Enum::name).toArray(String[]::new);
  //  }
  public static String[] getAlgos()
  {
    return JWSAlgorithm.Family.SIGNATURE.parallelStream().map(JWSAlgorithm::getName).toArray(String[]::new);
  }

  public static JWSVerifier getVerifier(JWSAlgorithm algo, Key key) throws JOSEException
  {

    if (JWSAlgorithm.Family.EC.contains(algo))
    {

      if (key instanceof ECPublicKey)
      {
        return new ECDSAVerifier((ECPublicKey) key);

      }
      else
      {
        throw new JOSEException("Invalid Key Type " + key.getAlgorithm()
            + " not supported by the ECDSASigner; note that DSA Keys are not currently supported by this processor.");
      }

    }

    else if (JWSAlgorithm.Family.RSA.contains(algo))
    {
      if (key instanceof RSAPublicKey)
      {
        return new RSASSAVerifier((RSAPublicKey) key);
      }
      else
      {
        throw new JOSEException("Invalid Key Type " + key.getAlgorithm()
            + " not supported by the RSASSASigner; note that DSA Keys are not currently supported by this processor.\n\n"
            + "  Use the following to create an RSA Key:\n\n"
            + "     keytool -genkeypair -alias jwtkey -keyalg RSA -dname \"CN=Server,OU=Unit,O=Organization,L=City,S=State,C=US\" -keypass pa55word -keystore kafka.client.keystore.jks -storepass pa55word\n");
      }

    }
    else if (JWSAlgorithm.Family.HMAC_SHA.contains(algo))
    {
      if (key instanceof SecretKey)
      {
        return new MACVerifier((SecretKey) key);
      }
      else
      {
        throw new JOSEException("Invalid Key Type " + key.getAlgorithm()
            + " not supported by the MACSigner; note that DSA Keys are not currently supported by this processor.");
      }
    }

    return null;
  }

  public static JWSSigner getSigner(JWSAlgorithm algo, Key key) throws JOSEException
  {

    if (JWSAlgorithm.Family.EC.contains(algo))
    {

      if (key instanceof ECPrivateKey)
      {
        return new ECDSASigner((ECPrivateKey) key);

      }
      else
      {
        throw new JOSEException("Invalid Key Type " + key.getAlgorithm()
            + " not supported by the ECDSASigner; note that DSA Keys are not currently supported by this processor.");
      }

    }

    else if (JWSAlgorithm.Family.RSA.contains(algo))
    {
      if (key instanceof PrivateKey)
      {
        return new RSASSASigner((PrivateKey) key);
      }
      else
      {
        throw new JOSEException("Invalid Key Type " + key.getAlgorithm()
            + " not supported by the RSASSASigner; note that DSA Keys are not currently supported by this processor.\n\n"
            + "  Use the following to create an RSA Key:\n\n"
            + "     keytool -genkeypair -alias jwtkey -keyalg RSA -dname \"CN=Server,OU=Unit,O=Organization,L=City,S=State,C=US\" -keypass pa55word -keystore kafka.client.keystore.jks -storepass pa55word\n");
      }

    }
    else if (JWSAlgorithm.Family.HMAC_SHA.contains(algo))
    {
      if (key instanceof SecretKey)
      {
        return new MACSigner((SecretKey) key);
      }
      else
      {
        throw new JOSEException("Invalid Key Type " + key.getAlgorithm()
            + " not supported by the MACSigner; note that DSA Keys are not currently supported by this processor.");
      }
    }

    return null;
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

  public static Key getPublicKey(SSLContextService sslService, String alias)
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException
  {
    FileInputStream is = new FileInputStream(sslService.getKeyStoreFile());

    KeyStore keystore = KeyStore.getInstance(sslService.getKeyStoreType());
    keystore.load(is, sslService.getKeyStorePassword().toCharArray());

    //    String alias = sslService.getIdentifier(); //"myalias";

    Key key = keystore.getKey(alias, sslService.getKeyPassword().toCharArray());
    if (key instanceof PrivateKey)
    {
      // Get certificate of public key
      java.security.cert.Certificate cert = keystore.getCertificate(alias);

      // Get public key
      PublicKey publicKey = cert.getPublicKey();

      return publicKey;
      // Return a key pair
      //      new KeyPair(publicKey, (PrivateKey) key);
    }

    return null;

  }

  public static Key getPrivateKey(SSLContextService sslService, String alias)
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException, UnrecoverableKeyException
  {
    FileInputStream is = new FileInputStream(sslService.getKeyStoreFile());

    KeyStore keystore = KeyStore.getInstance(sslService.getKeyStoreType());
    keystore.load(is, sslService.getKeyStorePassword().toCharArray());

    //    String alias = sslService.getIdentifier(); //"myalias";

    Key key = keystore.getKey(alias, sslService.getKeyPassword().toCharArray());
    //    if (key instanceof PrivateKey) {
    //      // Get certificate of public key
    //      Certificate cert = keystore.getCertificate(alias);
    //
    //      // Get public key
    //      PublicKey publicKey = cert.getPublicKey();
    //
    //      // Return a key pair
    //      new KeyPair(publicKey, (PrivateKey) key);
    //    }

    return key;

  }

  @Override public void init(final ProcessorInitializationContext context)
  {
    List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(SSL_CONTEXT);
    properties.add(JWT_KEY_ALGO);
    properties.add(JWT_KEY_ALIAS);
    properties.add(JWT_ATTRIBUTE_NAME);
    properties.add(JWT_TIME_TO_LIVE_MS);
    //    properties.add(JWT_SUBJECT_JSON_PATH);
    //    properties.add(JWT_ORGANIZATION_JSON_PATH);
    //    properties.add(JWT_BIZ_CONTEXT_JSON_PATH);

    this.properties = Collections.unmodifiableList(properties);

    Set<Relationship> relationships = new HashSet<>();
    relationships.add(FAILURE);
    relationships.add(SUCCESS);
    this.relationships = Collections.unmodifiableSet(relationships);
  }

  @Override public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue,
                                           final String newValue)
  {
    if (descriptor.equals(SSL_CONTEXT) || descriptor.equals(JWT_KEY_ALIAS) || descriptor.equals(JWT_KEY_ALGO))
    {
      sslService = null;
      signingKey = null;
      keyAlias = null;
      keyAlgo = null;
    }

    if (descriptor.equals(JWT_TIME_TO_LIVE_MS))
    {
      jwtTTL = -2;
    }

    if (descriptor.equals(JWT_ATTRIBUTE_NAME))
    {
      jwtAttributeName = null;
    }
    //
    //    else if (descriptor.equals(JWT_SUBJECT_JSON_PATH)) {
    //      subjJsonPathStr = newValue;
    //      subjJsonPath = JsonPath.compile(newValue);
    //    }
    //
    //    else if (descriptor.equals(JWT_ORGANIZATION_JSON_PATH)) {
    //      orgJsonPathStr = newValue;
    //      orgJsonPath = JsonPath.compile(newValue);
    //    }
    //
    //    else if (descriptor.equals(JWT_BIZ_CONTEXT_JSON_PATH)) {
    //      bizCtxJsonPathStr = newValue;
    //      bizCtxJsonPath = JsonPath.compile(newValue);
    //    }
    //

  }

  @Override public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException
  {
    final ComponentLog log = this.getLogger();
    FlowFile flowfile = session.get();

    if (flowfile == null)
    {
      session.transfer(flowfile, FAILURE);
      return;

    }

    if (jwtAttributeName == null)
    {
      jwtAttributeName = context.getProperty(JWT_ATTRIBUTE_NAME).getValue();
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
        signingKey = getPrivateKey(sslService, keyAlias);
      }
      if (keyAlgo == null)
      {
        keyAlgo = JWSAlgorithm.parse(context.getProperty(JWT_KEY_ALGO).getValue());
      }
      if (jwtTTL == -2)
      {
        jwtTTL = context.getProperty(JWT_TIME_TO_LIVE_MS).asInteger();
      }
      //      JsonNode jsonJWTData = null;

      boolean useAttributes = !(jwtAttributeName == null || jwtAttributeName.length() == 0);

      String jsonData = null;
      if (!useAttributes)
      {
        jsonData = StringReplacer.readString(session, flowfile);
      }
      else
      {
        jsonData = flowfile.getAttribute(jwtAttributeName);
        //        ObjectMapper mapper = new ObjectMapper();
        //        jsonJWTData = mapper.readValue(jsonStr, JsonNode.class);
      }

      JWTClaimsSet rawClaims = JWTClaimsSet.parse(jsonData);

      JWTClaimsSet.Builder builder = new JWTClaimsSet.Builder(rawClaims);

      if (jwtTTL > 0 && rawClaims.getExpirationTime() == null)
      {
        builder.expirationTime(new Date(new Date().getTime() + jwtTTL));
      }

      JWTClaimsSet claims = builder.build();

      if (claims.getClaim(subjJsonPathStr) == null)
      {
        log.error("Failed to get field " + subjJsonPathStr + " from payload");
        session.transfer(flowfile, FAILURE);
        return;
      }
      if (claims.getClaim(orgJsonPathStr) == null)
      {
        log.error("Failed to get field " + orgJsonPathStr + " from payload");
        session.transfer(flowfile, FAILURE);
        return;
      }
      if (claims.getClaim(bizCtxJsonPathStr) == null)
      {
        log.error("Failed to get field " + bizCtxJsonPathStr + " from payload");
        session.transfer(flowfile, FAILURE);
        return;
      }

      String claimsStr = StringReplacer.replaceAll(claims.toString(), "\\/", "/");

      Payload payload = new Payload(claimsStr);
      //      docCtx = JsonPath.parse(jsonJWTData.toString());
      //      String subj = docCtx.read(subjJsonPath);
      //      String org = docCtx.read(orgJsonPath);
      //      String bizCtx = docCtx.read(bizCtxJsonPath);

      //      Payload payload = new Payload("Hello, world!");
      PrivateKey key = (PrivateKey) getPrivateKey(sslService, keyAlias);
      RSASSASigner signer = new RSASSASigner((PrivateKey) key);
      JWSHeader header = new JWSHeader(keyAlgo);

      JWSObject jwsObject = new JWSObject(header, payload);
      jwsObject.sign(signer);

      String compactJws = jwsObject.serialize();

      if (useAttributes)
      {
        flowfile = writeDataToAttrib(flowfile, session, jwtAttributeName, compactJws);
      }
      else
      {
        flowfile = writeDataToFile(flowfile, session, compactJws);
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

  @Override public Set<Relationship> getRelationships()
  {
    return relationships;
  }

  @Override public List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    return properties;
  }

}
