# frozen_string_literal: true

require 'csv'
require 'json'
require 'pathname'

DATA_PROCESSING_OUTPUTS_PATH = Pathname.new(ARGV.shift)

puts %w[sample filename ntsm_score ntsm_result].join("\t")

CSV.table(DATA_PROCESSING_OUTPUTS_PATH, col_sep: "\t", quote_char: "\x00").each do |row|
  sample = row[:sample]
  outputs = JSON.parse(row[:workflow_outputs])['outputs']
  case row[:workflow_name]
  when 'data_processing/hifi_qc_workflow'
    summary_path = outputs['hifi_qc_wf.hifi_qc_summary']
    summary = CSV.table(summary_path, col_sep: "\t")
    filename = summary[:filename]
    ntsm_score = summary[:ntsm_score]
    ntsm_result = summary[:ntsm_result]
  when 'data_processing/hic_qc_workflow'
    summary_path = outputs['hic_qc_wf.hic_qc_summary']
    summary = CSV.table(summary_path, col_sep: "\t")
    filename = summary[:file_name]
    ntsm_score = summary[:ntsm_score]
    ntsm_result = summary[:ntsm_result]
  else
    warn "Unsupported workflow type: #{row[:workflow_name]}"
    exit 1
  end

  puts [sample, filename, ntsm_score, ntsm_result].join("\t")
end
