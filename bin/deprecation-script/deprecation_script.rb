require 'rubygems'
require 'net/http'
require 'json'
require 'logger'

require "./constants"
require "./failed_build_task"
require "./deprecation_email"



class DeprecationScript


  # Public: Constructor - initialize Logger for logging.
  #
  # script_root_dir - The root directory of the script.
  # audit_log_file - The file to audit log input.
  # failed_build_tasks_output_file - The file to output all failed build tasks
  #                                  info for debugging.
  # gen_email_output_dir - The directory to output generated emails.
  # failed_build_tasks_filter_file - The file consisting of failed build tasks
  #                                  that should not be sent (usually because
  #                                  we've sent them already).
  # build_request_id - The build request ID.
  # logger_file - The file to output Logger info.
  def initialize(script_root_dir,
                 audit_log_file,
                 failed_build_tasks_output_file,
                 gen_email_output_dir,
                 failed_build_tasks_filter_file,
                 build_request_id,
                 logger_file)

    @audit_log_file = audit_log_file
    @failed_build_tasks_output_file = failed_build_tasks_output_file
    @gen_email_output_dir = gen_email_output_dir
    @build_request_id = build_request_id

    # Initialize the filter filename before we change the working directory
    @failed_build_tasks_filter_file = "#{Dir.pwd}/#{failed_build_tasks_filter_file}"

    # Change the working directory
    Dir.chdir(script_root_dir)
    puts "Changed working directory to: #{Dir.pwd}"

    # Initialize the Logger
    @logger = Logger.new(logger_file)
    @logger.datetime_format = "%H:%M:%S"
    @logger.level = Logger::INFO
  end


  # Public: Calls the OwnershipService's getOwnedEntityById operation to
  #         retrieve ownership information of a package.
  #
  # package_name - The package name.
  #
  # Returns a HTTP response of the request.
  def get_ownership_information(package_name)
    endpoint = $OWNERSHIP_SERVICE_GET_OWNED_ENTITY_BY_ID_ENDPOINT
    endpoint = endpoint.sub(/%package_name%/, package_name)

    uri = URI(endpoint)
    Net::HTTP.get_response(uri)
  end


  # Public: Retrieves ownership information for each FailedBuildTask and
  #         returns the same array of FailedBuildTasks with ownership
  #         information included.
  #
  # failed_build_tasks - The array of FailedBuildTasks.
  #
  # Returns a modified array of FailedBuildTasks with ownership information
  # included.
  def append_ownership_info!(failed_build_tasks)
    @logger.info ''
    @logger.info '========================================'
    @logger.info 'Appending Ownership info to FailedBuildTasks...'
    @logger.info '========================================'

    failed_build_tasks.each_with_index do |failed_build_task, index|
      resp = get_ownership_information(failed_build_task.package_name)
      resp_body = JSON.parse(resp.body)

      unless resp_body['getOwnedEntityByIdResponse'].nil? || resp_body['getOwnedEntityByIdResponse']['getOwnedEntityByIdResult'].nil?
        result = resp_body['getOwnedEntityByIdResponse']['getOwnedEntityByIdResult']

        owner = result['owner']
        failed_build_task.manager           = owner['manager']
        failed_build_task.email_list        = owner['routineEmailList']
        failed_build_task.last_modified_by  = result['lastModifiedBy']

        @logger.info "#{index+1}/#{failed_build_tasks.size} Appended Ownership info to: #{failed_build_task.to_semantic_s}"
      end
    end

    failed_build_tasks
  end


  # Public: Get the HTML source of a build log from Brazil Makelog Website using
  #         Kerberos authentication.
  #
  # task_id - The task ID of the build task
  #
  # Returns the HTML source as a String.
  def bmw_get_build_log_html_source(task_id)
    curl_cmd = $CURL_KERBEROS_CMD_PREFIX + $BMW_TASK_ID_BUILD_LOG_ENDPOINT
    curl_cmd = curl_cmd.sub(/%task_id%/, task_id)

    `#{curl_cmd}`
  end


  # Public: Parse the HTML source of a build log and retrieves the download URL
  #         of the build log.
  #
  # html_source - The HTML source String
  #
  # Returns the download URL as a String.
  def bmw_parse_download_link(html_source)
    download_link_regex = Regexp.new($DOWNLOAD_LINK_REGEX)

    match_data = download_link_regex.match(html_source)

    if match_data.nil?
      nil
    else
      match_data[1]
    end
  end


  # Public: Get the plain text of the build log as a String from a download URL
  #         using Kerberos authentication.
  #
  # download_link - The download URL of the build log.
  #
  # Returns the plain text of the build log.
  def bmw_get_build_log_plain_text(download_link)
    curl_cmd = $CURL_KERBEROS_CMD_PREFIX + download_link

    `#{curl_cmd}`
  end


  # Public: Parse the audit log using regexp and returns an array of
  #         FailedBuildTasks.
  #
  # audit_log - The file name of the audit log.
  #
  # Returns an array of FailedBuildTasks.
  def parse_audit_log(audit_log)
    @logger.info ''
    @logger.info '========================================'
    @logger.info 'Parsing Audit Log to generate FailedBuildTasks...'
    @logger.info '========================================'

    f = File.new(audit_log)
    failed_build_tasks = Array.new
    failed_build_task_regex = Regexp.new($FAILED_BUILD_TASK_REGEX)

    f.each do |line|
      match_data = failed_build_task_regex.match(line)
      unless match_data.nil?
        task_id, package_name, version = match_data[1..3]

        failed_build_task = FailedBuildTask.new(
          @build_request_id, package_name, version, task_id)

        failed_build_tasks << failed_build_task
      end
    end

    f.close

    # Removes FailedBuildTasks that have the same package_name and version,
    # defined by to_semantic_s. This is a workaround considering that Ruby 1.8.7
    # doesn't have a good way to deal with this.
    unique_failed_build_tasks = Array.new
    existing_failed_build_tasks = Hash.new
    failed_build_tasks.each do |failed_build_task|
      if existing_failed_build_tasks[failed_build_task.to_semantic_s].nil?
        unique_failed_build_tasks << failed_build_task
        existing_failed_build_tasks[failed_build_task.to_semantic_s] = true # Set to non-nil
      end
    end

    unique_failed_build_tasks

    # Only works with Ruby 1.9+
    # Removes duplicate values in FailedBuildTasks using the block for
    # comparison, and returns a new Array
    # failed_build_tasks.uniq { |failed_build_task| failed_build_task.to_semantic_s }
  end


  # Public: Match deprecated APIs to each FailedBuildTask using regexp.
  #
  # failed_build_tasks - The Array of FailedBuildTasks.
  # api_regexps - The Hash mapping API descriptions to RegExp Objects.
  #
  # Returns an Array of FailedBuildTasks with matched APIs appended.
  # FailedBuildTasks without a match are deleted.
  def match_api_to_build_log_plain_texts(failed_build_tasks, api_regexps)
    @logger.info ''
    @logger.info '========================================'
    @logger.info 'Matching APIs to Build Logs...'
    @logger.info '========================================'

    failed_build_tasks.each_with_index do |failed_build_task, index|
      matched_apis = Array.new

      api_regexps.each do |api_desc, regexp|
        match_data = regexp.match(failed_build_task.build_log)
        matched_apis = matched_apis << api_desc unless match_data.nil?
      end

      # Only append matched APIs to the FailedBuildTask
      unless matched_apis.empty?
        failed_build_task.apis = matched_apis
        @logger.info "#{index+1}/#{failed_build_tasks.size} Matched APIs to #{failed_build_task.to_semantic_s}: #{matched_apis.join(', ')}"
      end
    end

    # Remove FailedBuildTasks that do not have a API match
    failed_build_tasks.delete_if { |failed_build_task|
      !failed_build_task.caused_by_using_deprecated_apis?
    }

    failed_build_tasks
  end


  # Public: Create a Hash mapping API description (key) to its
  #         RegExp Object (value).
  #
  # api_regexps - A Hash mapping API description (key) to the
  #               regular expression query (value).
  #
  # Returns a Hash mapping API description (key) to its RegExp Object (value).
  def create_api_regexps(deprecated_apis)
    api_regexps = Hash.new

    deprecated_apis.each { |api_desc, api_regexp_query|
      api_regexp_object = Regexp.new(api_regexp_query)
      api_regexps[api_desc] = api_regexp_object
    }

    api_regexps
  end


  # Public: Output all FailedBuildTasks into a file.
  #
  # failed_build_tasks - The list of FailedBuildTasks.
  # file - The output file.
  def output_failed_build_tasks(failed_build_tasks, file)
    @logger.info ''
    @logger.info '========================================'
    @logger.info "Writing FailedBuildTasks to #{file}..."
    @logger.info '========================================'

    # Write mode - creates the file if it does not exist or overwrites
    # the old version
    f = File.new(file, 'w')
    failed_build_tasks.each do |failed_build_task|
      f.puts "#{failed_build_task.to_output_file_format}"
      f.puts ''
    end
    f.close
  end


  # Public: Parses the list of FailedBuildTasks and generates a single
  #         DeprecationEmail for each task.
  #
  # failed_build_tasks - The list of FailedBuildTasks
  #
  # Returns an Array of DeprecationEmails.
  def generate_deprecation_emails(failed_build_tasks)
    @logger.info ''
    @logger.info '========================================'
    @logger.info 'Generating DeprecationEmails...'
    @logger.info '========================================'

    deprecation_emails = Array.new

    failed_build_tasks.each_with_index do |failed_build_task, index|
      deprecation_email = DeprecationEmail.new(failed_build_task)
      deprecation_email.format!
      deprecation_emails << deprecation_email

      @logger.info "#{index+1}/#{failed_build_tasks.size} Generated DeprecationEmail: #{deprecation_email}"
    end

    deprecation_emails
  end


  # Public: Writes each DeprecationEmail as a command into a file within a
  #         specified directory. This serves as a last check before sending
  #         them off.
  #
  # deprecation_emails - The Array of DeprecationEmails.
  # gen_email_output_dir - The directory to write to.
  def output_deprecation_emails(deprecation_emails, gen_email_output_dir)
    @logger.info ''
    @logger.info '========================================'
    @logger.info "Writing DeprecationEmails to #{gen_email_output_dir}"
    @logger.info '========================================'

    Dir.mkdir(gen_email_output_dir) unless File.directory? gen_email_output_dir

    deprecation_emails.each_with_index do |deprecation_email, index|
      @logger.info "#{index+1}/#{deprecation_emails.size} Writing DeprecationEmail: #{deprecation_email}"

      mail_cmd = deprecation_email.to_mail_command

      filename = File.join(gen_email_output_dir, "email#{index+1}.txt")
      f = File.new(filename, 'w')
      f.puts mail_cmd
      f.close
    end
  end


  # Public: Retrieve and append plain text build log to each FailedBuildTask.
  #
  # failed_build_tasks - The Array of FailedBuildTasks.
  #
  # Returns the modified FailedBuildTasks Array with build log information
  # added. Each build log has its trailing backslashes removed.
  def append_build_log_plain_texts(failed_build_tasks)
    @logger.info ''
    @logger.info '========================================'
    @logger.info 'Appending Build Logs to FailedBuildTasks...'
    @logger.info '========================================'

    failed_build_tasks.each_with_index do |failed_build_task, index|
      build_log_html_source = bmw_get_build_log_html_source(failed_build_task.task_id)
      if build_log_html_source.nil?
        @logger.error "Error getting build log html source for #{failed_build_task.to_semantic_s}"
        next
      end

      build_log_download_link = bmw_parse_download_link(build_log_html_source)
      if build_log_download_link.nil?
        @logger.error "Error getting build log download link for #{failed_build_task.to_semantic_s}"
        next
      end

      build_log_plain_text = bmw_get_build_log_plain_text(build_log_download_link)
      if build_log_plain_text.nil?
        @logger.error "Error getting build log plain text for #{failed_build_task.to_semantic_s}"
        next
      end

      # Remove all trailing backslashes from the build log
      build_log_plain_text = build_log_plain_text.gsub(/\\$/, '')

      failed_build_task.build_log = build_log_plain_text

      @logger.info "#{index+1}/#{failed_build_tasks.size} Appended Build Log to #{failed_build_task.to_semantic_s}"
    end

    failed_build_tasks
  end


  # Public: Print Summary of deprecated APIs still in use.
  #
  # failed_build_tasks - The Array of FailedBuildTasks.
  def print_api_occurrences(failed_build_tasks)
    @logger.info ''
    @logger.info '========================================'
    @logger.info 'Printing Summary of deprecated APIs still in use...'
    @logger.info '========================================'

    api_counts = Hash.new

    failed_build_tasks.each do |failed_build_task|
      failed_build_task.apis.each do |api|
        if api_counts[api].nil?
          api_counts[api] = 1
        else
          api_counts[api] += 1
        end
      end
    end

    api_counts.each do |key, value|
      @logger.info "#{value} - #{key}"
    end

  end


  # Public: Filters out (delete) FailedBuildTasks from the passed in
  #         FailedBuildTasks that are specified in the filter file.
  #
  # failed_build_tasks - The original FailedBuildTasks array.
  # failed_build_tasks_filter_file - The file containing the failed build tasks
  #                                  to filter out.
  #
  # Returns a modified FailedBuildTasks with tasks specified in the filter file
  # removed.
  def filter_failed_build_tasks!(failed_build_tasks, failed_build_tasks_filter_file)
    @logger.warn ''
    @logger.warn '========================================'
    @logger.warn 'Filtering out FailedBuildTasks...'
    @logger.warn '========================================'

    orig_tasks = failed_build_tasks

    # Create an Array of Strings from filter file
    filter_tasks = Array.new
    filter_file = File.new(failed_build_tasks_filter_file, 'r')
    filter_file.each do |line|
      matched_filter = /^(.+)( #)?/.match(line) # Only single-line comments are supported, must begin with '#'
      filter_tasks << matched_filter[1] unless matched_filter.nil?
    end
    filter_file.close

    # Remove FailedBuildTasks that are specified in the filter file
    failed_build_tasks.delete_if { |failed_build_task|
      if filter_tasks.include? failed_build_task.to_semantic_s
        @logger.warn "Filtered: #{failed_build_task.to_semantic_s}"
        true
      else
        false
      end
    }

    failed_build_tasks
  end


  # Public: Begin execution of the deprecation script.
  #
  # 1. Parse the audit log file and generate an Array of FailedBuildTasks.
  # 2. Retrieve ownership information for each FailedBuildTask.
  # 3. Retrieve build log (as plain text) for each FailedBuildTask.
  # 4. Parse the build log of using RegExps of the deprecated APIs.
  # 5. Filter out FailedBuildTasks that are not caused by removal of deprecated
  #    APIs.
  # 6. Create a DeprecationEmail for each FailedBuildTask.
  # 7. Output each DeprecationEmail to the generated email output dir.
  #    (deprecation_mailer.rb handles the actual sending of emails)
  def begin
    # Obtain an array of FailedBuildTasks
    failed_build_tasks = parse_audit_log(@audit_log_file)

    # Filter out FailedBuildTasks that are specified in the filter file
    failed_build_tasks = filter_failed_build_tasks!(failed_build_tasks, @failed_build_tasks_filter_file)

    # Retrieve and append ownership information to each FailedBuildTask
    failed_build_tasks = append_ownership_info!(failed_build_tasks)

    # Retrieve and append plain text build log to each FailedBuildTask
    failed_build_tasks = append_build_log_plain_texts(failed_build_tasks)

    # Create a Hash mapping API description (key) to its RegExp Object (value)
    # so it can be reused for parsing each FailedBuildTask
    api_regexps = create_api_regexps($DEPRECATED_APIS)

    # Match APIs to build log in each FailedBuildTask using regexp
    failed_build_tasks = match_api_to_build_log_plain_texts(failed_build_tasks, api_regexps)

    # We now have FailedBuildTasks that are caused by removal of deprecated APIs
    # Write each FailedBuildTask to a output file for debugging
    output_failed_build_tasks(failed_build_tasks, @failed_build_tasks_output_file)

    # Print Summary of deprecated APIs still in use
    print_api_occurrences(failed_build_tasks)

    # Generate an array of DeprecationEmails from the array of FailedBuildTasks
    deprecation_emails = generate_deprecation_emails(failed_build_tasks)

    # Write each DeprecationEmail as a file containing the mail cmd to execute
    # into the gen email output directory
    output_deprecation_emails(deprecation_emails, @gen_email_output_dir)
  end


end



# Deprecation Script Entry Point
if __FILE__ == $0
  deprecation_script = DeprecationScript.new(
    $SCRIPT_ROOT_DIR,
    $AUDIT_LOG_FILE,
    $FAILED_BUILD_TASKS_OUTPUT_FILE,
    $GEN_EMAIL_OUTPUT_DIR,
    $GLOBAL_FAILED_BUILD_TASKS_EXCLUSIONS_FILE,
    $BUILD_REQUEST_ID,
    $LOGGER_OUTPUT_FILE)

  puts 'Beginning DeprecationScript...'
  puts "Script root dir: #{$SCRIPT_ROOT_DIR}"
  deprecation_script.begin
  puts 'Completed DeprecationScript!'
end
