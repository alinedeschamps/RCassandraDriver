connect <- function(address) 
{
  if (!is.character(address))
  {
    stop("Address must be a character string")
  }
  code <- .Call("connect", address)
  if (code == 0) 
  {
    print("Connection to Cassandra successful")
  }
  else 
  {
    print("Cannot connect to Cassandra")
  }
  return(code)
}

connectWithCredentials <- function(address, username, password) 
{
  if (!is.character(address))
  {
    stop("Address must be a character string")
  }
  
  if (!is.character(username))
  {
    stop("Username must be a character string")
  }
  
  if (!is.character(password))
  {
    stop("Password must be a character string")
  }
  
  code <- .Call("connectWithCredentials", address, username, password)
  if (code == 0) 
  {
    print("Connection to Cassandra successful")
  }
  else 
  {
    print("Cannot connect to Cassandra")
  }
  return(code)
}

disconnect <- function() 
{
  .Call("disconnect")
}

executeQuery <- function(query) 
{
  if (!is.character(query))
  {
    stop("Query must be a character string")
  }
  
  result <- .Call("executeQuery", query)
  data <- listToDataFrame(result)
  return(data)
}

listToDataFrame <- function(list) {
  ul <- unlist(list)
  nc <- length(list[[1]])
  m <- matrix(ul, ncol = nc, byrow = TRUE)
  D <- data.frame(m[-(1:2), ], stringsAsFactors = FALSE)
  colnames(D) <- m[1, ]
  S <- setColClasses(D, m[2, ])
}