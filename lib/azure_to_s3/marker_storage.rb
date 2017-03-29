require 'fileutils'

module AzureToS3
  class MarkerStorage
    attr_reader :marker

    def initialize(file_name)
      @file_name = file_name
      @marker = File.read(@file_name) if File.exist?(@file_name)
    end

    def marker=(marker)
      if marker
        File.open(@file_name, 'w') {|f| f << marker }
      else
        FileUtils.rm_f(@file_name)
      end
      @marker = marker
    end
  end
end
