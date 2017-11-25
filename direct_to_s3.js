// geography all merged together, saved as single CSV file
// geography loaded / immediately converted to key-val lookup on Lambda or Elasticache

// data files combined by seq/moe parsed to csv and immediately uploaded to s3

// retrieval will go from tile geo -> geoLambda (single) -> dataLambda (multiple)
