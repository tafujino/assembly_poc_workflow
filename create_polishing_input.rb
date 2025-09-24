# frozen_string_literal: true

require 'csv'
require 'fileutils'
require 'json'
require 'pathname'

HIC_HIFIASM_OUTPUTS_TABLE_PATH = Pathname.new(ARGV.shift)
READS_TABLE_PATH = Pathname.new(ARGV.shift)
CHECKPOINT_PATH = Pathname.new(ARGV.shift)
OUT_BASE_DIR = Pathname.new(ARGV.shift).expand_path

hic_hifiasm_outputs_table = CSV.table(HIC_HIFIASM_OUTPUTS_TABLE_PATH, col_sep: "\t", quote_char: "\x00")

reads_table = CSV.table(READS_TABLE_PATH, col_sep: "\t")
reads_by_sample = reads_table.group_by { |row| row[:sample] }

puts %w[sample workflow_name input_path].join("\t")

hic_hifiasm_outputs_table.each do |row|
  sample = row[:sample]
  out_dir = OUT_BASE_DIR / sample / 'polishing'
  FileUtils.mkpath(out_dir)

  outputs = JSON.parse(row[:workflow_outputs])['outputs']

  hap1_path = outputs['hicHifiasmAssembly.hap1FastaGz']
  hap2_path = outputs['hicHifiasmAssembly.hap2FastaGz']

  ont_reads, hifi_reads = %w[UL HiFi].map do |type|
    paths = reads_by_sample[sample].filter_map { |row| row[:path] if row[:type] == type }
    if paths.empty?
      warn "#{type} reads is empty (sample = #{sample})"
      exit 1
    end
    paths
  end

  input = {
    'hprc_DeepPolisher.Hap1RawFasta': hap1_path,
    'hprc_DeepPolisher.Hap2RawFasta': hap2_path,
    'hprc_DeepPolisher.DeepPolisherModelFilesTarGZ': CHECKPOINT_PATH,
    'hprc_DeepPolisher.ONTReads': ont_reads,
    'hprc_DeepPolisher.HifiReads': hifi_reads,
    'hprc_DeepPolisher.sampleName': sample
  }

  input_path = out_dir / "#{sample}.json"
  File.write(input_path, JSON.pretty_generate(input))
  puts [
    sample,
    'polishing/deeppolisher',
    input_path
  ].join("\t")
end
