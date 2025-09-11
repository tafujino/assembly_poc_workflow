# frozen_string_literal: true

require 'csv'
require 'json'
require_relative 'cromwell_server'

NETWORK_CONF_PATH = 'network.json'

network_conf = JSON.parse(File.read(NETWORK_CONF_PATH))
cromwell = CromwellServer.new(network_conf['host'], network_conf['port'])

WORKFLOW_SUBMISSION_TABLE_PATH = ARGV.shift

puts %w[sample workflow_name input_path workflow_id workflow_status workflow_outputs].join("\t")
CSV.table(WORKFLOW_SUBMISSION_TABLE_PATH, col_sep: "\t").each do |row|
  status = cromwell.status(row[:workflow_id])['status']
  puts [
    row[:sample],
    row[:workflow_name],
    row[:input_path],
    row[:workflow_id],
    status,
    JSON.generate(cromwell.outputs(row[:workflow_id]))
  ].join("\t")
end
