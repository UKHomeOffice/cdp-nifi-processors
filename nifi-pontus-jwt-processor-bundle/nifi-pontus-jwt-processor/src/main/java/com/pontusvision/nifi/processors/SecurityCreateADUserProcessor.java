/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pontusvision.nifi.processors;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import javax.naming.Context;
import javax.naming.NameAlreadyBoundException;
import javax.naming.NamingException;
import javax.naming.directory.*;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import java.io.IOException;
import java.util.*;

//import io.jsonwebtoken.Jwts;
//import io.jsonwebtoken.SignatureAlgorithm;

/**
 * @author Leo Martins
 */
@Tags({ "pontus", "Active Directory", "AD",
    "Security" }) @CapabilityDescription("Creates an active directory user.") public class SecurityCreateADUserProcessor
    extends AbstractProcessor
{

  public static final PropertyDescriptor USER_NAME = new PropertyDescriptor.Builder().name("New User Name")
      .description("The new User name to be added to AD.").defaultValue("testuser1").required(true)
      .expressionLanguageSupported(true).addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
      .build();
  public static final PropertyDescriptor USER_PASSWD = new PropertyDescriptor.Builder().name("New User Password")
      .description("The new User's password to be added to AD.").defaultValue("testuser1£££££6666777erKK")
      .required(true).expressionLanguageSupported(true)
      .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).build();
  public static final PropertyDescriptor USER_GROUP = new PropertyDescriptor.Builder().name("New User Group")
      .description("The new User's AD group.").defaultValue("CN=Administrators,CN=Builtin,DC=test2,DC=local")
      .required(true).expressionLanguageSupported(true)
      .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).build();
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
  final static Validator adCredentialsValidator = new Validator()
  {
    @Override public ValidationResult validate(String subject, String input, ValidationContext context)
    {
      ValidationResult.Builder builder = new ValidationResult.Builder();

      boolean isValid = false;

      String message = "Incompatible values for TLS Key, alias, and sign algorithm:";
      try
      {
        String domainName = context.getProperty(DOMAIN_NAME).getValue();
        String domainRoot = context.getProperty(DOMAIN_ROOT).getValue();
        String domainURL = context.getProperty(DOMAIN_URL).getValue();
        String adminNameDN = context.getProperty(ADMIN_NAME).getValue();
        String adminPasswd = context.getProperty(ADMIN_PASSWD).getValue();

        NewUser user = new NewUser(domainName, domainRoot, domainURL, adminNameDN, adminPasswd, "test", "test", "test");
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
  public static final PropertyDescriptor DOMAIN_NAME = new PropertyDescriptor.Builder()
      .name("Active Directory Domain Name").description("The AD domain name where the user will be created. ")
      .defaultValue("TEST2.LOCAL").required(true).expressionLanguageSupported(false)
      .addValidator(adCredentialsValidator).build();
  public static final PropertyDescriptor DOMAIN_ROOT = new PropertyDescriptor.Builder()
      .name("Active Directory Domain Root").description("The AD domain root where the user will be created. ")
      .defaultValue("CN=Users,DC=test2,DC=local").required(true).expressionLanguageSupported(false)
      .addValidator(adCredentialsValidator).build();
  public static final PropertyDescriptor DOMAIN_URL = new PropertyDescriptor.Builder()
      .name("Active Directory Domain URL").description("The AD domain URL where the user will be created. ")
      .defaultValue("ldaps://test2-ad.test2.local:636").required(true).expressionLanguageSupported(false)
      .addValidator(adCredentialsValidator).build();
  public static final PropertyDescriptor ADMIN_NAME = new PropertyDescriptor.Builder()
      .name("Active Directory Admin User")
      .description("The AD domain admin user to create the user (in distinguished name format).")
      .defaultValue("CN=leo.martins,CN=Users,DC=test2,DC=local").required(true).expressionLanguageSupported(false)
      .addValidator(adCredentialsValidator).build();
  public static final PropertyDescriptor ADMIN_PASSWD = new PropertyDescriptor.Builder()
      .name("Active Directory Admin Password")
      .description("The AD domain admin user to create the user (in distinguished name format).")
      .defaultValue("HE!pa55word0").required(true).sensitive(true).expressionLanguageSupported(false)
      .addValidator(adCredentialsValidator).build();
  protected List<PropertyDescriptor> properties;
  protected Set<Relationship> relationships;

  @Override public void init(final ProcessorInitializationContext context)
  {
    List<PropertyDescriptor> properties = new ArrayList<>();
    properties.add(DOMAIN_NAME);
    properties.add(DOMAIN_ROOT);
    properties.add(DOMAIN_URL);
    properties.add(ADMIN_NAME);
    properties.add(ADMIN_PASSWD);
    properties.add(USER_NAME);
    properties.add(USER_PASSWD);
    properties.add(USER_GROUP);

    this.properties = Collections.unmodifiableList(properties);

    Set<Relationship> relationships = new HashSet<>();
    relationships.add(FAILURE);
    relationships.add(SUCCESS);
    this.relationships = Collections.unmodifiableSet(relationships);
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

    String domainName = context.getProperty(DOMAIN_NAME).getValue();
    String domainRoot = context.getProperty(DOMAIN_ROOT).getValue();
    String domainURL = context.getProperty(DOMAIN_URL).getValue();
    String adminNameDN = context.getProperty(ADMIN_NAME).getValue();
    String adminPasswd = context.getProperty(ADMIN_PASSWD).getValue();
    String userName = context.getProperty(USER_NAME).evaluateAttributeExpressions(flowfile).getValue();
    String userPassd = context.getProperty(USER_PASSWD).evaluateAttributeExpressions(flowfile).getValue();
    String userGroup = context.getProperty(USER_GROUP).evaluateAttributeExpressions(flowfile).getValue();

    try
    {
      NewUser user = new NewUser(domainName, domainRoot, domainURL, adminNameDN, adminPasswd, userName, userPassd,
          userGroup);
      user.addUser();

    }
    catch (NameAlreadyBoundException e)
    {
      log.debug("Failed to add new user; user already exists");

    }
    catch (Exception e)
    {
      log.error("Failed to add new user: ", e);
      session.transfer(flowfile, FAILURE);
      return;

    }

    session.transfer(flowfile, SUCCESS);

    //    session.remove(flowfile);
    //    session.commit();

  }

  //  @Override
  //  public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
  //    if (descriptor.equals(SSL_CONTEXT) || descriptor.equals(HASH_KEY_ALIAS) || descriptor.equals(HASH_KEY_ALGO)) {
  //      sslService = null;
  //      signingKey = null;
  //      keyAlias = null;
  //      keyAlgo = null;
  //    }
  //
  //    if (descriptor.equals(HASH_ENCRYPTED_STRING_ATTRIBUTE_NAME)) {
  //      hashEncryptedStrAttributeName = null;
  //    }

  //  }

  //  public static FlowFile writeDataToAttrib(FlowFile flowFile, ProcessSession session, final String jwtAttributeName, final String data) {
  //    flowFile = session.putAttribute(flowFile, jwtAttributeName, data);
  //    return flowFile;
  //  }
  //
  //  public static FlowFile writeDataToFile(FlowFile flowfile, ProcessSession session, final String data) {
  //
  //    flowfile = session.write(flowfile, new OutputStreamCallback() {
  //
  //      @Override
  //      public void process(OutputStream out) throws IOException {
  //        out.write(data.getBytes());
  //      }
  //    });
  //
  //    return flowfile;
  ////    session.transfer(flowfile, SUCCESS);
  //
  //  }
  //

  @Override public Set<Relationship> getRelationships()
  {
    return relationships;
  }

  @Override public List<PropertyDescriptor> getSupportedPropertyDescriptors()
  {
    return properties;
  }

  public static class NewUser
  {

    private String userName, password, domainName, domainRoot, domainURL, adminNameDN, adminPasswd, groupDn;
    private LdapContext context;

    public NewUser(String domainName, String domainRoot, String domainURL, String adminNameDN, String adminPasswd,
                   String userName, String password, String userGroup) throws NamingException
    {
      this.domainName = domainName;
      this.domainRoot = domainRoot;
      this.domainURL = domainURL;
      this.adminNameDN = adminNameDN;
      this.adminPasswd = adminPasswd;
      this.userName = userName;
      this.password = password;
      this.groupDn = userGroup;

      Hashtable<String, String> env = new Hashtable<String, String>();

      env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");

      // set security credentials, note using simple cleartext authentication
      env.put(Context.SECURITY_AUTHENTICATION, "simple");
      env.put(Context.SECURITY_PRINCIPAL, this.adminNameDN);
      env.put(Context.SECURITY_CREDENTIALS, this.adminPasswd);

      // connect to my domain controller
      env.put(Context.PROVIDER_URL, this.domainURL);
      this.context = new InitialLdapContext(env, null);

    }

    public boolean addUser() throws NamingException, IOException
    {

      // Create a container set of attributes
      Attributes container = new BasicAttributes();

      // Create the objectclass to add
      Attribute objClasses = new BasicAttribute("objectClass");
      objClasses.add("top");
      objClasses.add("person");
      objClasses.add("organizationalPerson");
      objClasses.add("user");

      // Assign the username, first name, and last name
      //      String cnValue = new StringBuffer(firstName).append(" ").append(lastName).toString();
      Attribute cn = new BasicAttribute("cn", userName);
      Attribute sAMAccountName = new BasicAttribute("sAMAccountName", userName);
      Attribute principalName = new BasicAttribute("userPrincipalName", userName + "@" + domainName);
      //      Attribute givenName = new BasicAttribute("givenName", firstName);
      //      Attribute sn = new BasicAttribute("sn", lastName);
      Attribute uid = new BasicAttribute("uid", userName);

      //some useful constants from lmaccess.h
      final int UF_NORMAL_ACCOUNT = 0x0200;
      final int UF_DONT_EXPIRE_PASSWD = 0x10000;

      final int UF_ACCOUNTDISABLE = 0x0002;
      final int UF_PASSWD_NOTREQD = 0x0020;
      final int UF_PASSWD_CANT_CHANGE = 0x0040;
      final int UF_PASSWORD_EXPIRED = 0x800000;

      //Note that you need to create the user object before you can
      //set the password. Therefore as the user is created with no
      //password, user AccountControl must be set to the following
      //otherwise the Win2K3 password filter will return error 53
      //unwilling to perform.

      //      Attribute userAccountControlInit = new BasicAttribute("userAccountControl",Integer.toString(UF_NORMAL_ACCOUNT + UF_PASSWD_NOTREQD + UF_PASSWORD_EXPIRED+ UF_ACCOUNTDISABLE));
      //      Attribute userPassword = new BasicAttribute("userPassword", Base64.getMimeDecoder().decode(password));
      Attribute userPassword = new BasicAttribute("userPassword", Base64.getMimeDecoder().decode(password));
      //      Attribute userPassword = new BasicAttribute("userPassword",  password);

      // Add these to the container
      container.put(objClasses);
      container.put(sAMAccountName);
      container.put(principalName);
      container.put(cn);
      //      container.put(sn);
      //      container.put(givenName);
      container.put(uid);
      container.put(userPassword);
      String userDn = getUserDN(userName);

      //      StartTlsResponse tls = (StartTlsResponse)context.extendedOperation(new StartTlsRequest());
      //      tls.negotiate();

      // Create the entry
      context.createSubcontext(userDn, container);

      ModificationItem[] mods = new ModificationItem[1];
      Attribute mod = new BasicAttribute("member", userDn);
      mods[0] = new ModificationItem(DirContext.ADD_ATTRIBUTE, mod);
      context.modifyAttributes(groupDn, mods);

      Attribute userAccountControl = new BasicAttribute("userAccountControl", Integer.toString(UF_NORMAL_ACCOUNT));
      Attributes postCreation = new BasicAttributes();

      postCreation.put(userAccountControl);
      //      postCreation.put(userPassword);

      context.modifyAttributes(userDn, DirContext.REPLACE_ATTRIBUTE, postCreation);
      // Add password

      //      tls.close();
      context.close();

      return true;
    }

    private String getUserDN(String aUsername)
    {
      return "cn=" + aUsername + "," + domainRoot;
    }
  }

}
