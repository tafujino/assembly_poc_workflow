# frozen_string_literal: true

require 'csv'
require 'json'
require_relative 'cromwell_server'

ORIGINAL_REPO_BASE_URL = 'https://raw.githubusercontent.com/human-pangenomics/hpp_production_workflows/0dc24c75ea3349bc7763ec71607f216d2822d440'

# network.json is like the following:
# { "host": "xxx.xxx.xxx.xxx", "port": "xxxx" }
NETWORK_CONF_PATH = 'network.json'

network_conf = JSON.parse(File.read(NETWORK_CONF_PATH))

workflow_url_table = {
  'data_processing/hic_qc_workflow' => "#{ORIGINAL_REPO_BASE_URL}/data_processing/wdl/workflows/hic_qc_workflow.wdl",
  'data_processing/hifi_qc_workflow' => "#{ORIGINAL_REPO_BASE_URL}/data_processing/wdl/workflows/hifi_qc_workflow.wdl",
  'assembly/hic_hifi_assembly' => "#{ORIGINAL_REPO_BASE_URL}/assembly/wdl/workflows/hic_hifiasm_assembly_cutadapt_multistep.wdl",
  'polishing/deeppolisher' => "#{ORIGINAL_REPO_BASE_URL}/polishing/wdl/workflows/hprc_DeepPolisher.wdl"
  }

WORKFLOW_INPUT_TABLE_PATH = ARGV.shift
OPTIONS_PATH = Pathname.new('workflow_options.json')

cromwell = CromwellServer.new(network_conf['host'], network_conf['port'])

puts %w[sample workflow_name input_path workflow_id workflow_status].join("\t")
CSV.table(WORKFLOW_INPUT_TABLE_PATH, col_sep: "\t").each do |row|
  workflow_type = row[:workflow_name]
  workflow_url = workflow_url_table[workflow_type]
  unless workflow_url
    warn "Undefined workflow type: #{workflow_type} (sample = #{row[:sample]}, input_path = #{row[:input_path]})"
    next
  end
  ret = cromwell.submit_by_url(workflow_url, row[:input_path], OPTIONS_PATH)
  puts [
    row[:sample],
    row[:workflow_name],
    row[:input_path],
    ret['id'],
    ret['status']
  ].join("\t")
end
