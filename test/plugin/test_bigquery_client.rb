require 'helper'

class BigQueryClientTest < Test::Unit::TestCase
  def setup
    @client = Fluent::BigQueryPlugin::BigQueryClient.new(
      project:                '4242424242',
      dataset:                'my_dataset',
      email:                  '1234567890@developer.gserviceaccount.com',
      private_key_path:       '/path/to/keyfile.p12',
      private_key_passphrase: 'itsasecret',
      auth_method:            'my_auth_method'
    )
  end

  def test_create_table
    mock(@client) do |client|
      client.bigquery { mock!.tables.mock!.insert }
      client.access_api.with_any_args { mock(Object.new).error? { false } }
    end
    schema = [{'name' => 'foo', 'type' => 'timestamp'}, {'name' => 'bar', 'type' => 'string'}]
    assert { @client.create_table('my_table', schema).nil? }
  end

  def test_insert
    mock(@client) do |client|
      client.bigquery { mock!.tabledata.mock!.insert_all }
      client.access_api.with_any_args { mock(Object.new).error? { false } }
    end
    rows = [{'json' => {'a' => 'b'}}, {'json' => {'b' => 'c'}}]
    assert { @client.insert('my_table', rows).nil? }
  end

  def test_fetch_schema
    input = {
      schema: {
        fields: [
          { name: 'time', type: 'TIMESTAMP' },
          { name: 'tty',  type: 'STRING'    }
        ]
      }
    }
    expected = [
      { 'name' => 'time', 'type' => 'TIMESTAMP' },
      { 'name' => 'tty',  'type' => 'STRING'    }
    ]
    result = Object.new
    mock(result) do |result|
      result.error? { false }
      result.body { JSON.generate(input) }
    end
    mock(@client) do |client|
      client.bigquery { mock!.tables.mock!.get }
      client.access_api.with_any_args { result }
    end
    assert { @client.fetch_schema('my_table') == expected }
  end

  def test_not_found
    errors = [
      { code: 404, klass: Fluent::BigQueryPlugin::NotFound        },
      { code: 409, klass: Fluent::BigQueryPlugin::Conflict        },
      { code: 403, klass: Fluent::BigQueryPlugin::ClientError     },
      { code: 503, klass: Fluent::BigQueryPlugin::ServerError     },
      { code: 301, klass: Fluent::BigQueryPlugin::UnexpectedError }
    ]
    errors.each do |error|
      result = Object.new
      mock(result) do |result|
        result.error? { true }
        result.status { error[:code] }
        result.error_message { 'this is an error message' }
      end
      mock(@client) do |client|
        client.bigquery { mock!.tabledata.mock!.insert_all }
        client.access_api.with_any_args { result }
      end
      rows = [{'json' => {'a' => 'b'}}, {'json' => {'b' => 'c'}}]
      assert_raise(error[:klass]) do
        @client.insert('my_table', rows)
      end
    end
  end
end
