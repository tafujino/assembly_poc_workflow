# frozen_string_literal: true

require 'csv'
require_relative 'cromwell_server'

# SubmissionResult = Struct.new(:sample, :workflow, :json_path, :status)

GITHUB_REPO_BASE_URL = 'https://raw.githubusercontent.com/human-pangenomics/hpp_production_workflows/0dc24c75ea3349bc7763ec71607f216d2822d440'
HOST = '127.0.0.1'
PORT = '33061'

workflow_url_table = {
  'data_processing/hic_qc_workflow' => "#{GITHUB_REPO_BASE_URL}/data_processing/wdl/workflows/hic_qc_workflow.wdl",
  'data_processing/hifi_qc_workflow' => "#{GITHUB_REPO_BASE_URL}/data_processing/wdl/workflows/hic_qc_workflow.wdl",
}

WORKFLOW_INPUTS_TABLE_PATH = ARGV.shift
OPTIONS_PATH = Pathname.new('workflow_options.json')

cromwell = CromwellServer.new(HOST, PORT)

CSV.table(WORKFLOW_INPUTS_TABLE_PATH, col_sep: "\t").each do |row|
  workflow_type = row[:workflow]
  unless workflow_url_table.key?(workflow_type)
    warn "Undefined workflow type: #{workflow_type} (sample = #{row[:sample]}, json_path = #{row[:json_path]})"
    next
  end
end
