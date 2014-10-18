require 'net/http'
require 'uri'
require 'json'
require 'logger'
#require 'pry'
#require 'pry-debugger'

DOCS_LOCATION = "./docs"
PRODUCTION = "http://node01.public.learningregistry.net"
SANDBOX = "http://sandbox.learningregistry.org"
SLICE_ENDPOINT = "/slice"
LOG_FILE = "./report.log"

@count = 0
@logger = Logger.new(LOG_FILE)
@uri = nil
@http = nil
@request = nil

def crawl(environment = "production", resume_token = nil)
	environment = PRODUCTION if environment.downcase == "production"
	endpoint = environment + SLICE_ENDPOINT
	endpoint << "?resumption_token=#{resume_token}" if resume_token

	loop_count = 0

	@uri = URI.parse(endpoint)
	@http = Net::HTTP.new(@uri.host, @uri.port)
	@request = Net::HTTP::Get.new(@uri.request_uri)
	@logger.info @uri

	while loop_count < 10 do
		response = @http.request(@request)
		#binding.pry
		if response.code == "200" && response.body.length > 0 then
			json = JSON.parse(response.body)
			@logger.info ("Processed: #{@count}, processing #{json["documents"].length} more.")
			@logger.info ("Resume token #{json["resumption_token"]}") if json["resumption_token"]
			json["documents"].each do |doc|
				begin
					filename = DOCS_LOCATION + "/" + doc["doc_ID"] + ".json"
					File.open(filename, 'w') {|f| f.write(doc.to_json) } unless File.exists? filename
				rescue Exception => e
					@logger.error "Error parsing document #{doc["doc_ID"]}, message: " + e.message
				end
			end
			@count += json["documents"].length
			puts "Processed #{@count} documents"
			@uri = nil
			@http = nil
			@request = nil
			token = json["resumption_token"]
			json = nil
			crawl(environment, token) if token
		else
			if response.code != "200" then
				@logger.error("HTTP error, response code: #{response.code}, loop count #{loop_count}, resume_token: #{resume_token}")
				loop_count += 1
				sleep(5)
			else
				exit
			end
		end
	end
end

def ensure_docs_dir
	Dir.mkdir(DOCS_LOCATION) unless File.directory?(DOCS_LOCATION)
end

ensure_docs_dir
crawl("production", "eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJzdGFydGtleSI6IDEzMTk3ODE1MjgsICJlbmRrZXkiOiAxNDEzNjUzNzk5LCAic3RhcnRrZXlfZG9jaWQiOiAiYTZhNmRhODU1ZjU0NGM1ZjhjNmJiZGVjOWU4ZjMzMDQifQ.Q_m1X7-ETSGvYaMrPfBVtEkuwnxSnPeYyNENkDUMyxg")
