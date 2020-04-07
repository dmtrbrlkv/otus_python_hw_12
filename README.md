## Loading data into memcache

### Requirements

  protobuf

    go get -u github.com/golang/protobuf/protoc-gen-go

  memcache

    go get memcache


### Compiling
    go build

### Run
    ./memcache

### Usage:

  -adid string
  
    adid memcache address
      
  -dvid string
  
    dvid memcache address
      
  -gaid string
  
    gaid memcache address
      
  -idfa string
  
    idfa memcache address
      
  -p string
  
    log file pattern

  -c int

    chanel size (default 3000000)
    
  -b int
   
    buffer size (default 0)

      
