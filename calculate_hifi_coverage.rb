# frozen_string_literal: true

require 'csv'
require 'json'
require 'pathname'

GENOME_GBP_SIZE = 3.1

DATA_PROCESSING_OUTPUTS_PATH = Pathname.new(ARGV.shift)

puts %w[sample coverage].join("\t")
CSV.table(DATA_PROCESSING_OUTPUTS_PATH, col_sep: "\t", quote_char: "\x00").group_by { |row| row[:sample] }.each do |sample, rows|
  hifi_rows = rows.select { |row| row[:workflow_name] == 'data_processing/hifi_qc_workflow' }
  total_num_bases = hifi_rows.sum do |row|
    outputs = JSON.parse(row[:workflow_outputs])
    summary_path = outputs['outputs']['hifi_qc_wf.hifi_qc_summary']
    total_bp = CSV.table(summary_path, col_sep: "\t").first[:total_bp].to_i
  end
  coverage = total_num_bases / (3.1 * 1_000_000_000)
  coverage_int = (coverage + 0.5).to_i
  puts [sample, coverage_int].join("\t")
end
