class DeprecationEmail


  attr_reader :failed_build_task, :build_request_id
  attr_accessor :title, :to, :cc, :body


  def initialize(failed_build_task)
    @failed_build_task  = failed_build_task
    @title              = $EMAIL_TEMPLATE[:title]
    @cc                 = $EMAIL_TEMPLATE[:cc]
    @from               = $EMAIL_TEMPLATE[:from]
    @body               = $EMAIL_TEMPLATE[:body]
  end


  # Public: Formats the arguments for the mail command. The core arguments are
  #         title (subject), to, and body.
  def format!
    format_title!
    format_to!
    format_body!
  end


  def format_title!
    @title = @title.gsub($EMAIL_TEMPLATE_PACKAGE_NAME_REGEX, @failed_build_task.package_name)
    @title = @title.gsub($EMAIL_TEMPLATE_VERSION_REGEX, @failed_build_task.version)
  end


  def format_to!
    to_emails = Array.new
    to_emails << @failed_build_task.email_list unless @failed_build_task.email_list.nil?
    to_emails << "#{@failed_build_task.manager}@amazon.com" unless @failed_build_task.manager.nil?
    to_emails << "#{@failed_build_task.last_modified_by}@amazon.com" unless @failed_build_task.last_modified_by.nil?
    @to = to_emails.join(', ')
  end


  def format_body!
    @body = @body.gsub($EMAIL_TEMPLATE_BUILD_REQUEST_ID_REGEX, @failed_build_task.build_request_id)
    @body = @body.gsub($EMAIL_TEMPLATE_PACKAGE_NAME_REGEX, @failed_build_task.package_name)
    @body = @body.gsub($EMAIL_TEMPLATE_TASK_ID_REGEX, @failed_build_task.task_id)

    formatted_apis = ""
    @failed_build_task.apis.flatten!
    @failed_build_task.apis.each do |api|
      formatted_apis += "* #{api}\n"
    end

    @body = @body.gsub($EMAIL_TEMPLATE_API_REGEX, formatted_apis)
  end


  # Public: Formats this object to a ready-to-execute 'mail' command, that when
  #         executed, will send the email.
  #
  # Returns a formatted ready-to-execute 'mail' command as a String.
  def to_mail_command
    @to = "nobody@amazon.com" # TEST SENDING TO YOURSELF FIRST!

    "echo \"#{@body}\" | mail -s \"#{@title}\" -c #{@cc} \"#{@to}\" -f #{@from}"
  end


  def to_s
    "title: #{@title}, to: #{@to}, cc: #{@cc}, from: #{@from}"
  end


end
