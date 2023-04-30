require "functions_framework"
require "google/cloud/bigquery"

# Register an HTTP function with the Functions Framework
FunctionsFramework.cloud_event "main" do |request|
  KEYS = %w[num_ebikes_available station_status is_installed num_bikes_disabled num_bikes_available num_docks_disabled is_returning is_renting eightd_has_available_keys legacy_id num_docks_available last_reported station_id requested_at]

  response = Faraday.get('https://gbfs.citibikenyc.com/gbfs/en/station_status.json')
  json = JSON.parse(response.body)
  station_statuses = json.dig('data', 'stations')
  requested_at = Time.now.utc.strftime('%Y-%m-%dT%H:%M:%S')

  bigquery = Google::Cloud::Bigquery.new project: "lucybikes"
  dataset  = bigquery.dataset 'lucysight'
  table    = dataset.table 'station_statuses'

  row_data = station_statuses.map { |station_status| station_status.slice(*KEYS).merge(requested_at:) }

  puts "Discovered #{row_data.count} rows"

  response = table.insert row_data

  puts response.insert_errors[0..10].map { |error| error.errors }

  if response.success?
    puts "Inserted rows successfully"
  else
    puts "Failed to insert #{response.error_rows.count} rows"
  end
end
