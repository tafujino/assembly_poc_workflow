# frozen_string_literal: true

require 'csv'
require 'fileutils'
require 'json'
require 'pathname'

HicReadMatch = Struct.new(:prefix, :read_num, :postfix, :path)

# @param read_paths [Array<String>]
# @return [Array<Array<String>>] read1 paths and read2 paths
def organize_hic_reads(read_paths)
  read_matches = read_paths.map do |path|
    unless path =~ /^(.+)_(R[12])_(\d+\.fastq\.gz)$/
      warn "Unexpected Hi-C read path: #{path}"
      exit 1
    end
    HicReadPathMatch.new(prefix: Regexp.last_match(1), read_num: Regexp.last_match(2), postfix: Regexp.last_match(3), path: path)
  end.group_by(&:read_num)
  read1_matches = read_matches['R1']
  read2_matches = read_matches['R2']

  read1_paths = []
  read2_paths = []
  read1_matches.each do |match1|
    match2 = read2_matches.find { |match| match.prefix == match1.prefix && match.postfix == match1.postfix }
    unless match2
      warn "Cannot find corresponding R2 for R1 #{match1.path}"
      exit 1
    end
    read2_matches.delete(match2)
    read1_paths.push(match1.path)
    read2_paths.push(match2.path)
  end
  unless read2_matches.empty?
    warn 'Found unpaired R2:'
    read2_matches.each do |match|
      warn match.path
    end
    exit 1
  end
  [read1_paths, read2_path]
end

ASM_READS_TABLE_PATH = Pathname.new(ARGV.shift)
SEX_TABLE_PATH = Pathname.new(ARGV.shift)

asm_reads_table = CSV.table(ASM_READS_TABLE_PATH, col_sep: "\t")
sex_table = CSV.table(SEX_TABLE_PATH, col_sep: "\t")

asm_reads_by_sample = asm_reads_table.group_by { |row| row[:sample] }
sex_by_sample = sex_table.group_by { |row| row[:sample] }.map.to_h do |sample, rows|
  if rows.length > 1
    warn "Multiple sex assignments for sample #{sample}"
    exit 1
  end
  row = rows.first
  [sample, row[:sex].downcase]
end

puts %w[sample workflow_name input_path].join("\t")

asm_reads_by_sample.filter_map do |sample, row|
  out_dir = OUT_BASE_DIR / sample / 'hic_hifiasm_assembly'
  FileUtils.mkpath(out_dir)

  asm_reads_by_type = row.group_by { |row| row[:type] }
  asm_read_paths_by_type = asm_reads_by_sample.transform_values { |rows| rows.map { |row| row[:path] } }

  unless asm_read_paths_by_type['HiC']
    warn "HiC read is empty (sample: #{sample})"
    exit 1
  end
  hic_reads1, hic_reads2 = organize_hic_reads(asm_read_paths_by_type['HiC'])

  unless asm_read_paths_by_type['HiFi']
    warn "HiFi read is empty (sample: #{sample})"
    exit 1
  end
  hifi_reads = asm_reads_paths_by_type['HiFi']

  ont_reads = asm_reads_paths_by_type['UL'] || []

  unless sex_by_sample.key?(sample)
    warn "Cannot determine the sex of sample #{sample}"
    exit 1
  end
  input = {
    'hicHifiasmAssembly.childID': sample,
    'hicHifiasmAssembly.isMaleSample': sex_by_sample[sample] == 'male' : true ? false,

  }
  input_path = out_dir / "#{sample}.json"
  File.write(input_path, JSON.pretty_generate(input))
  puts [
    workflow_input.sample,
    'assembly/hic_hifi_assembly',
    input_path
  ].join("\t")
end
