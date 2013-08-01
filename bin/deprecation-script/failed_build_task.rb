class FailedBuildTask


  attr_accessor :build_request_id, :package_name, :version, :task_id, :email_list, :manager, :last_modified_by, :apis, :build_log


  def initialize(build_request_id, package_name, version, task_id)
    @build_request_id = build_request_id
    @package_name = package_name
    @version = version
    @task_id = task_id
  end


  def to_s
    result = "#{@package_name}-#{@version}"

    result += ", task_id: #{@task_id}" unless task_id.nil?

    result += ", email_list: #{@email_list}" unless email_list.nil?

    result += ", manager: #{@manager}" unless manager.nil?

    result += ", last_modified_by: #{@last_modified_by}" unless last_modified_by.nil?

    result += ", apis: #{@apis}" unless apis.nil?
  end


  def to_output_file_format
    result = "========== #{self.to_semantic_s} =========="

    result += "\ntask_id: #{@task_id}" unless task_id.nil?

    result += "\nemail_list: #{@email_list}" unless email_list.nil?

    result += "\nmanager: #{@manager}" unless manager.nil?

    result += "\nlast_modified_by: #{@last_modified_by}" unless last_modified_by.nil?

    result += "\napis: #{@apis}" unless apis.nil?

    unless build_log.nil?
      result += "\n===== BUILD LOG ====="
      result += "\n#{@build_log}"
    end

    result += "\n========================================"
  end


  def to_semantic_s
    "#{@package_name}-#{@version}"
  end


  def caused_by_using_deprecated_apis?
    !apis.nil? && !apis.empty?
  end


end
