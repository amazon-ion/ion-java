module Constants

  # If true, real emails are sent out! That is, the mail command is executed.
  # If false, the mailer will NOT send real emails out.
  $SEND_REAL_EMAILS = false

  # The build request ID of the dry-run build request.
  $BUILD_REQUEST_ID = '1031411011'

  # Directory to store output files from a single DeprecationScript run.
  # This directory also contains the audit log file that serves as the entry
  # point for the DeprecationScript.
  $SCRIPT_ROOT_DIR = "run_#{$BUILD_REQUEST_ID}"

  # File consisting of the audit log of the package build request (this is the
  # file for the entry point of the script)
  $AUDIT_LOG_FILE = 'audit_log.txt'

  # File to output all failed build tasks info for debugging
  $FAILED_BUILD_TASKS_OUTPUT_FILE = "failed_build_tasks_output.txt"

  # File consisting of failed build tasks that should not be sent (usually
  # because we've sent them already)
  $GLOBAL_FAILED_BUILD_TASKS_EXCLUSIONS_FILE = "global_build_tasks_to_exclude.txt"

  # Directory to output generated emails
  $GEN_EMAIL_OUTPUT_DIR = "gen_emails"

  # Deprecation script's log file
  $LOGGER_OUTPUT_FILE = "script_log.txt"

  # OwnershipService getOwnedEntityById
  $OWNERSHIP_SERVICE_GET_OWNED_ENTITY_BY_ID_ENDPOINT = "http://ownership-service.amazon.com/\?id\=brazil%2Fpackage%2F%package_name%\&Operation\=getOwnedEntityById\&ContentType\=JSON"

  # Brazil Makelog Website endpoint
  $BMW_TASK_ID_BUILD_LOG_ENDPOINT = "https://devcentral.amazon.com/ac/brazil/BMW\?BTMTaskID\=%task_id%"

  # Regexp for matching against Audit Log
  $FAILED_BUILD_TASK_REGEX = /Build task (\d+) for (.+)-([0-9.]+) on platform [a-zA-Z0-9_]+ now FAILED$/

  # Regexp for matching download URL within Brazil Makelog Website HTML source
  $DOWNLOAD_LINK_REGEX = /<a href="(https:\/\/.*.log)">Download<\/a>/

  # Prefix for curl command, for authenticating with Internal Amazon's Kerberos
  $CURL_KERBEROS_CMD_PREFIX = "curl --negotiate -u : -k "

  # Hash mapping deprecated APIs (key) to regular expression queries (value).
  $DEPRECATED_APIS = {
    # Methods
    "com.amazon.ion.IonReader.hasNext() -- Since 2009"                                  =>   /\[javac\].*\.hasNext\(/,
    "com.amazon.ion.ValueFactory.newList(Collection) -- Since R10"                      =>   /\[javac\].*\.newList\(/,
    "com.amazon.ion.ValueFactory.newSexp(Collection) -- Since R10"                      =>   /\[javac\].*\.newSexp\(/,
    "com.amazon.ion.SymbolTable.addSymbol(String) -- Since R15"                         =>   /\[javac\].*\.addSymbol\(/,
    "com.amazon.ion.SymbolTable.findSymbol(int) -- Since R15"                           =>   /\[javac\].*\.findSymbol\(/,
    "com.amazon.ion.IonWriter.addTypeAnnotationId(int) -- Since R15"                    =>   /\[javac\].*\.addTypeAnnotationId\(/,
    "com.amazon.ion.IonWriter.setFieldId(int) -- Since R15"                             =>   /\[javac\].*\.setFieldId\(/,
    "com.amazon.ion.IonWriter.setTypeAnnotationIds(int…) -- Since R15"                  =>   /\[javac\].*\.setTypeAnnotationIds\(/,
    "com.amazon.ion.IonWriter.writeSymbol(int) -- Since R15"                            =>   /\[javac\].*\.writeSymbol\(/,
    "com.amazon.ion.IonReader.getFieldId() -- Since R15"                                =>   /\[javac\].*\.getFieldId\(/,
    "com.amazon.ion.IonValue.getFieldId() -- Since R15"                                 =>   /\[javac\].*\.getFieldId\(/,
    "com.amazon.ion.IonReader.getSymbolId() -- Since R15"                               =>   /\[javac\].*\.getSymbolId\(/,
    "com.amazon.ion.IonSymbol.getSymbolId() -- Since R15"                               =>   /\[javac\].*\.getSymbolId\(/,
    "com.amazon.ion.IonReader.getTypeAnnotationIds() -- Since R15"                      =>   /\[javac\].*\.getTypeAnnotationIds\(/,
    "com.amazon.ion.IonReader.iterateTypeAnnotationIds() -- Since R15"                  =>   /\[javac\].*\.iterateTypeAnnotationIds\(/,
    "com.amazon.ion.util.Printer.printJson(IonValue, Appendable) -- Since 2007"         =>   /\[javac\].*\.printJson\(/,
    "com.amazon.ion.IonWriter.writeTimestamp(Date, Integer) -- Since 2009"              =>   /\[javac\].*\.writeTimestamp\(/,
    "com.amazon.ion.IonWriter.writeTimestampUTC(Date) -- Since 2009"                    =>   /\[javac\].*\.writeTimestampUTC\(/,
    "com.amazon.ion.IonWriter.writeValue(IonValue) -- Since R13"                        =>   /\[javac\].*\.writeValue\(/,
    "com.amazon.ion.IonDatagram.toBytes() -- Since 2009"                                =>   /\[javac\].*\.toBytes\(/,
    "com.amazon.ion.IonDatagram.byteSize() -- To be deprecated in R17"                  =>   /\[javac\].*\.byteSize\(/,
    "com.amazon.ion.IonDatagram.getBytes(byte[]) -- To be deprecated in R17"            =>   /\[javac\].*\.getBytes\(/,
    "com.amazon.ion.IonDatagram.getBytes(byte[], int) -- To be deprecated in R17"       =>   /\[javac\].*\.getBytes\(/,
    "com.amazon.ion.IonValue.getTypeAnnotationStrings() -- Since 2008"                  =>   /\[javac\].*\.getTypeAnnotationStrings\(/,
    "com.amazon.ion.IonValue.deepMaterialize() -- Since 2009"                           =>   /\[javac\].*\.deepMaterialize\(/,
    "com.amazon.ion.IonValue.getFieldNameId() -- Since R15"                             =>   /\[javac\].*\.getFieldNameId\(/,
    "com.amazon.ion.IonBlob.appendBase64(Appendable) -- Since 2009"                     =>   /\[javac\].*\.appendBase64\(/,
    "com.amazon.ion.IonLob.newBytes() -- Since 2009"                                    =>   /\[javac\].*\.newBytes\(/,
    "com.amazon.ion.IonSymbol.intValue() -- Since 2008"                                 =>   /\[javac\].*\.intValue\(/,
    "com.amazon.ion.IonDecimal.toBigDecimal() -- Since 2008"                            =>   /\[javac\].*\.toBigDecimal\(/,
    "com.amazon.ion.IonFloat.toBigDecimal() -- Since 2008"                              =>   /\[javac\].*\.toBigDecimal\(/,
    "com.amazon.ion.IonInt.toBigInteger() -- Since 2008"                                =>   /\[javac\].*\.toBigInteger\(/,

    # Classes
    "com.amazon.ion.util.Printer.JsonPrinterVisitor -- Since 2008"                      =>   /import com.amazon.ion.util.Printer.JsonPrinterVisitor/,
    "com.amazon.ion.system.SystemFactory -- Since R10"                                  =>   /import com.amazon.ion.system.SystemFactory/,
    "com.amazon.ion.util.Text -- Since 2009"                                            =>   /import com.amazon.ion.util.Text/,

    # Interfaces
    "com.amazon.ion.IonTextReader -- Since R13"                                         =>   /import com.amazon.ion.IonTextReader/,
    "com.amazon.ion.SystemSymbolTable -- Since R13"                                     =>   /import com.amazon.ion.SystemSymbolTable/
  }


  $EMAIL_TEMPLATE_BUILD_REQUEST_ID_REGEX  = /%build_request_id%/
  $EMAIL_TEMPLATE_PACKAGE_NAME_REGEX      = /%package_name%/
  $EMAIL_TEMPLATE_VERSION_REGEX           = /%version%/
  $EMAIL_TEMPLATE_EMAIL_LIST_REGEX        = /%email-list%/
  $EMAIL_TEMPLATE_MANAGER_REGEX           = /%manager%/
  $EMAIL_TEMPLATE_LAST_MODIFIED_BY_REGEX  = /%last_modified_by%/
  $EMAIL_TEMPLATE_TASK_ID_REGEX           = /%task_id%/
  $EMAIL_TEMPLATE_API_REGEX               = /%apis%/

  $EMAIL_TEMPLATE = {
    :title                       => "[Action Required] %package_name%-%version% -- IonJava API Deprecation",
    :cc                          => "nobody@amazon.com", # CC field for each generated email - use your email
    :from                        => "nobody@amazon.com", # FROM field for each generated email - use your email
    :body                        => "Hello,

You are receiving this email because you are listed as the owner of (or a recent committer to) the package %package_name%, which is a consumer of IonJava.

%package_name%: https://devcentral.amazon.com/ac/brazil/directory/package/overview/%package_name%

This package is using deprecated APIs of IonJava that the Ion Team intends to remove in our next release, R17. Removal of deprecated APIs is part of our long-standing effort to improve the quality of the library.

We ran a dry-run build request with the deprecated APIs removed. Packages that are still using these APIs will throw a compiler error and be reflected as failed builds in our dry-run build request.

The dry-run build request: https://build.amazon.com/%build_request_id%
This package's failed build log: https://devcentral.amazon.com/ac/brazil/BMW?BTMTaskID=%task_id%

Based off the build log of this package build, we are able to identify that these deprecated APIs are still in use:

%apis%

Here's what you need to do:
1. Remove the usage of these deprecated APIs from this package. The complete list of deprecated APIs (link below) has suggestions for replacements.
2. Pull IonJava/cleanup remote branch's tip (link below) into your Brazil workspace, this is essentially a patch to IonJava/release with the deprecated APIs removed. Note: this is NOT to be used anywhere else except for testing removal of deprecation APIs.
3. Submit a dry-run build request consisting of only IonJava/cleanup and this package. If the dry-run build succeeds, you have successfully removed any offending usage of the deprecated APIs. If not, go back to (1).
4. Depending on your release cycle of this package or preferably, ASAP, build this package with the changes into the live version set.

Complete list of deprecated APIs: https://devcentral.amazon.com/ac/brazil/package-master/package/live/IonJava-1.0/brazil-documentation/deprecated-list.html
IonJava/cleanup remote branch with deprecated APIs removed: https://code.amazon.com/packages/IonJava/logs/heads/cleanup

Right now, IonJava has 1423 direct consumers and many more transitive consumers, so please help us out. Doing so will greatly accelerate the timeline of R17 being released and benefit the entire Ion community.

I will be following up regularly to check in about the status of this package. Thank you.

Regards,
The Ion Team"
  }

end
