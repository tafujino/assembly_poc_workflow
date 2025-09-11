# frozen_string_literal: true

require 'csv'
require 'fileutils'
require 'json'
require 'pathname'

WorkflowInput = Struct.new(:sample, :workflow_name, :input_path)

# @param sample [String]
# @param hic_rows [Array<CSV::Row>]
# @param illumina_paths [Array<String>]
# @param out_dir [Pathname]
# @return [Array<WorkflowInput>]
def create_hic_qc_input(sample, hic_rows, illumina_paths, out_dir)
  hic_rows.map.with_index do |row, i|
    id = "#{sample}_hic_#{i}"
    hic_path = row[:path]
    input = {
      'hic_qc_wf.hic_reads': hic_path,
      'hic_qc_wf.other_reads': illumina_paths,
      'hic_qc_wf.file_id': id
    }
    input_path = out_dir / 'hic' / "#{id}.json"
    FileUtils.mkpath(input_path.dirname)
    File.write(input_path, JSON.pretty_generate(input))
    WorkflowInput.new(sample: sample, workflow_name: 'data_processing/hic_qc_workflow', input_path: input_path)
  end
end

# @param sample [String]
# @param hifi_rows [Array<CSV::Row>]
# @param illumina_paths [Array<String>]
# @param out_dir [Pathname]
# @return [Array<WorkflowInput>]
def create_hifi_qc_input(sample, hifi_rows, illumina_paths, out_dir)
  hifi_rows.map.with_index do |row, i|
    id = "#{sample}_hifi_#{i}"
    hifi_path = row[:path]
    input = {
      'hifi_qc_wf.hifi_reads': hifi_path,
      'hifi_qc_wf.other_reads': illumina_paths,
      'hifi_qc_wf.sample_id': id,
      'hifi_qc_wf.perform_methylation_check': false
    }
    input_path = out_dir / 'hifi' / "#{id}.json"
    FileUtils.mkpath(input_path.dirname)
    File.write(input_path, JSON.pretty_generate(input))
    WorkflowInput.new(sample: sample, workflow_name: 'data_processing/hifi_qc_workflow', input_path: input_path)
  end
end

# @param sample [String]
# @param ont_rows [Array<CSV::Row>]
# @param illumina_paths [Array<String>]
# @param out_dir [Pathname]
# @return [Array<WorkflowInput>]
def create_ont_qc_input(sample, ont_rows, illumina_paths, out_dir)
  ont_rows.map.with_index do |row, i|
    id = "#{sample}_ont_#{i}"
    ont_path = row[:path]
    input = {
      'ont_qc_wf.ont_reads': ont_path,
      'ont_qc_wf.other_reads': illumina_paths,
      'ont_qc_wf.file_id': id,
    }
    input_path = out_dir / 'ont' / "#{id}.json"
    FileUtils.mkpath(input_path.dirname)
    File.write(input_path, JSON.pretty_generate(input))
    WorkflowInput.new(sample: sample, workflow_name: 'data_processing/ont_qc_workflow', input_path: input_path)
  end
end

ASM_READS_TABLE_PATH = Pathname.new(ARGV.shift)
ILLUMINA_READS_TABLE_PATH = Pathname.new(ARGV.shift)
OUT_BASE_DIR = Pathname.new(ARGV.shift).expand_path

asm_reads_table = CSV.table(ASM_READS_TABLE_PATH, col_sep: "\t")
illumina_reads_table = CSV.table(ILLUMINA_READS_TABLE_PATH, col_sep: "\t")

asm_reads_by_sample = asm_reads_table.group_by { |row| row[:sample] }
illumina_reads_by_sample = illumina_reads_table.group_by { |row| row[:sample] }

workflow_inputs = asm_reads_by_sample.filter_map do |sample, row|
  out_dir = OUT_BASE_DIR / sample / 'data_processing'
  unless illumina_reads_by_sample.key?(sample)
    warn "Cannot find Illumina reads for #{sample}. Skips this sample."
    next
  end
  illumina_paths = illumina_reads_by_sample[sample].flat_map do |row|
    [row[:r1_path], row[:r2_path]]
  end

  asm_reads_by_type = row.group_by { |row| row[:type] }
  [
    create_hic_qc_input(sample, asm_reads_by_type['HiC'] || [], illumina_paths, out_dir),
    create_hifi_qc_input(sample, asm_reads_by_type['HiFi'] || [], illumina_paths, out_dir),
    create_ont_qc_input(sample, asm_reads_by_type['UL'] || [], illumina_paths, out_dir)
  ]
end.flatten

puts %w[sample workflow_name input_path].join("\t")
workflow_inputs.each do |workflow_input|
  puts [
    workflow_input.sample,
    workflow_input.workflow_name,
    workflow_input.input_path
  ].join("\t")
end
