module Fluent::BigQueryPlugin
  class BigQueryAPIError < StandardError
  end

  class ClientError < BigQueryAPIError
  end

  class NotFound < ClientError
  end

  # HTTP 409 Conflict
  class Conflict < ClientError
  end

  class ServerError < BigQueryAPIError
  end

  class UnexpectedError < BigQueryAPIError
  end
end
