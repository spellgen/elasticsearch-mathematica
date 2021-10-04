# elasticsearch-mathematica
Utilities for calling elasticsearch HTTP API from Mathematica

## Example call

Create a client (ec) by overriding the defaults (see the package). If username/password aren't defined you will be prompted for them on the first call.
This example runs a search over some specific document terms and a date range (but returns no documents of the actual search).

Aggregations are done per `user` in the index, further sub-aggregating on the range of `Timestamp` in the set.
The output is an association where in this case `res["aggregations"]["user"]["buckets"]` will hold the data.

    ec = ESClient["Domain" -> "my-es-cluster.home.net", "Scheme" -> "https",
      "VerifySecurityCertificates" -> False];

    range1 = {DateObject[{2021, 4, 1}], DateObject[{2021, 5, 1}]};

    qSeen2 = <|
      ESQuery[
        ESBoolQuery[
          ESBoolMust[{
            ESTermQuery["state", "TN"],
            ESTermQuery["county", "Knox"],
            ESRangeQuery["timestamp", UnixTime[range1[[1]]]*1000, UnixTime[range1[[2]]]*1000]
          }]
        ]
      ],
      ESAggs[
        ESTermsAggregation["user", "user_id", 5000]~ESSubAggs~
        ESAggs[
          ESMinAggregation["start", "timestamp"],
          ESMaxAggregation["end", "timestamp"]
        ]
      ],
      ESSize[0]
    |>
    
    res = ESPost[ec, "/index-base-*/_search", qSeen]
