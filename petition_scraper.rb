require "net/http"
require "uri"
require "json"
require 'rubygems'
require 'eventmachine'
require 'em-http'
require 'concurrent'

module PetitionScraper

  PETITIONS_URL = "https://petition.parliament.uk/petitions.json?page="
  MANY_SIGNATURE_THRESHOLD = 100000

  @@last_page_number = 0
  @@total_count = 0
  @@semaphore = Concurrent::Semaphore.new(0)
  @@http_label = 0
  @@urls = []
  @@many_signature_petitions_count = 0

  def get_total_signature_count()
    t0 = Time.now
    json = JSON.parse(get_petition_json("1"))
    @@last_page_number = get_last_page_number_from_json(json)
    for i in 1..@@last_page_number
      @@urls << (PETITIONS_URL + i.to_s)
    end
    puts "Calling multiple http requests"
    while (!@@urls.empty?)
      get_petition_count_async(@@urls)
      @@semaphore.acquire
    end
    puts "Time taken: %d" % (Time.now - t0)
    puts "Total signature count: %d" % @@total_count
    puts "%d petitions with over %d signatures and debated in the House of Commons" % [@@many_signature_petitions_count, MANY_SIGNATURE_THRESHOLD]
  end

  def get_petition_count_async(urls)
    EM.run do

      multi = EM::MultiRequest.new
      urls.each do |url|
        @@http_label += 1
        multi.add(@@http_label, EM::HttpRequest.new(url, :connect_timeout => 5, :inactivity_timeout => 10).get)
      end

      #Invokes when all requests have finished
      multi.callback do
        multi.responses[:callback].each do |http_request_number, callback|
          count = get_signature_count_from_json(JSON.parse(callback.response))
          @@total_count += count
          if count >= MANY_SIGNATURE_THRESHOLD
            @@many_signature_petitions_count += 1
          end
        end
        @@urls = []
        if multi.responses[:errback].size > 0
          puts "Failed: %d" % multi.responses[:errback].size
          multi.responses[:errback].values.each do |error|
            @@urls << error.conn.uri
          end
        end
        @@semaphore.release
        EM.stop
      end

    end
  end

  def get_petition_json(page_number)
    uri = URI.parse(PETITIONS_URL + page_number)
    response = Net::HTTP.get_response(uri)
    response.body
  end

  def get_signature_count_from_json(json)
    count = 0
    json["data"].each do |petition|
      count += petition["attributes"]["signature_count"]
    end
    count
  end

  def is_valid_json?(json)
      JSON.parse(json)
      return true
    rescue JSON::ParserError => e
      return false
  end

  def get_last_page_number_from_json(json)
    link = json["links"]["last"]
    index = link.index("=")
    link[(index+1)..-1].to_i
  end

end

#Script - Used in debugging
#include PetitionScraper
#get_total_signature_count()
