# frozen_string_literal: true

require 'csv'
require 'json'
require_relative 'cromwell_server'

# SubmissionResult = Struct.new(:sample, :workflow, :json_path, :status)

ORIGINAL_REPO_BASE_URL = 'https://raw.githubusercontent.com/human-pangenomics/hpp_production_workflows/0dc24c75ea3349bc7763ec71607f216d2822d440'

# network.json is like the following:
# { "host": "xxx.xxx.xxx.xxx", "port": "xxxx" }
NETWORK_CONF_PATH = 'network.json'

network_conf = JSON.parse(File.read(NETWORK_CONF_PATH))

workflow_url_table = {
  'data_processing/hic_qc_workflow' => "#{ORIGINAL_REPO_BASE_URL}/data_processing/wdl/workflows/hic_qc_workflow.wdl",
  'data_processing/hifi_qc_workflow' => "#{ORIGINAL_REPO_BASE_URL}/data_processing/wdl/workflows/hifi_qc_workflow.wdl",
}

WORKFLOW_INPUTS_TABLE_PATH = ARGV.shift
OPTIONS_PATH = Pathname.new('workflow_options.json')

cromwell = CromwellServer.new(network_conf['host'], network_conf['port'])

CSV.table(WORKFLOW_INPUTS_TABLE_PATH, col_sep: "\t").each do |row|
  workflow_type = row[:workflow]
  workflow_url = workflow_url_table[workflow_type]
  unless workflow_url
    warn "Undefined workflow type: #{workflow_type} (sample = #{row[:sample]}, json_path = #{row[:json_path]})"
    next
  end
  ret = cromwell.submit_by_url(workflow_url, row[:json_path], OPTIONS_PATH)
  pp ret
end
