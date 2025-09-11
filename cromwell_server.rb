# frozen_string_literal: true

require 'rest-client'
require 'json'

CROMWELL_API_VERSION = 'v1'

class CromwellServer
  # @param host [String]
  # @param port [String, Integer]
  def initialize(host, port)
    @host = host
    @port = port
    @uri_prefix = "http://#{@host}:#{@port}/api/workflows/#{CROMWELL_API_VERSION}"
  end

  # @param wdl_url [String]
  # @param inputs_path [String] json
  # @param options_path [String] json
  # @return [Hash]
  def submit_by_url(wdl_url, inputs_path, options_path)
    inputs_file = File.open(inputs_path, 'rb')
    options_file = File.open(options_path, 'rb')
    res = RestClient.post @uri_prefix,
                          { workflowType: 'WDL',
                            workflowUrl: wdl_url,
                            workflowInputs: inputs_file,
                            workflowOptions: options_file
                          }
    inputs_file.close
    options_file.close
    JSON.load(res.body)
  end

  # @param id [String]
  # @return [Hash]
  def status(id)
    begin
      res = RestClient.get "#{@uri_prefix}/#{id}/status"
      JSON.load(res.body)
    rescue RestClient::NotFound
      { 'id' => id, 'status' => 'Not Found' }
    end
  end

  # @param id [String]
  # @return [Hash]
  def outputs(id)
    begin
      res = RestClient.get "#{@uri_prefix}/#{id}/outputs"
      JSON.load(res.body)
    rescue RestClient::NotFound
      { 'id' => id, 'outputs' => nil }
    end
  end

  # @param id [String]
  # @return [Hash]
  def abort(id)
    begin
      res = RestClient.post "#{@uri_prefix}/#{id}/abort", {}
      JSON.load(res.body)
    rescue RestClient::NotFound
      { 'id' => id, 'status' => 'Not Found' }
    end
  end
end
