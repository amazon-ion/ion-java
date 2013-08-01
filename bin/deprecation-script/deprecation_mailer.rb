require 'rubygems'
require 'logger'

require "./constants"



class DeprecationMailer


  def initialize(script_root_dir, gen_email_output_dir, logger_file, send_real_emails)
    @gen_email_output_dir = gen_email_output_dir
    @send_real_emails = send_real_emails

    # Change the working directory
    Dir.chdir(script_root_dir)
    puts "Changed working directory to: #{Dir.pwd}"

    @logger = Logger.new(logger_file)
    @logger.datetime_format = "%H:%M:%S"
    @logger.level = Logger::INFO
  end


  # Public: Reads each email file (containing the mail command) and either
  #         prints or executes the mail command depending on the boolean flag
  #         send_real_emails.
  def send!
    @logger.warn ''
    @logger.warn '========================================'
    @logger.warn 'Sending DeprecationEmails...'
    @logger.warn '========================================'

    Dir.chdir(@gen_email_output_dir)
    @logger.warn "Changed Directory to: #{Dir.pwd}"

    Dir.glob("email[0-9]*.txt").each do |filename|
      mail_command = IO.read(filename)

      if @send_real_emails
        `#{mail_command}` # Sends the real email!
      else
        puts mail_command # Outputs to command line instead
      end

      matched_data = /mail -s \"\[Action Required\] (.+?) --/.match(mail_command)

      @logger.warn "Sent email! #{matched_data[1]}"
    end
  end


end



# Deprecation Mailer Entry Point
if __FILE__ == $0

  # DeprecationMailer handles sending the actual emails, we give it the
  # directory containing the generated emails
  deprecation_mailer = DeprecationMailer.new(
    $SCRIPT_ROOT_DIR,
    $GEN_EMAIL_OUTPUT_DIR,
    $LOGGER_OUTPUT_FILE,
    $SEND_REAL_EMAILS)

  puts 'Beginning DeprecationMailer...'
  puts "Script root dir: #{$SCRIPT_ROOT_DIR}"
  puts "Send real emails? #{$SEND_REAL_EMAILS}"

  if $SEND_REAL_EMAILS
    puts '=============== WARNING ==============='
    puts "This instance of DeprecationMailer will send REAL emails!"
    puts "If you don't want this to happen, terminate this script immediately!"
    puts "Set the flag accordingly in constants.rb's $SEND_REAL_EMAILS flag"
    puts '=============== WARNING ==============='
  end

  # Sleep for a period of time so the user can read the warning message
  sleep(5) # seconds

  # Send the emails! This is irreversible!
  deprecation_mailer.send!

  puts 'Completed DeprecationMailer!'
end
