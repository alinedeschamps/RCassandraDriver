setColClasses <- function(D, vColClasses)
{
	nl <- dim(D)[1]
	nc <- dim(D)[2]
	if(length(vColClasses)!=nc)
	{
		stop("Vector of classes must have the same length than the number of column of the data.frame")
	}
	
	S <- D
	
	for(i in 1:nc)
	{
		vc <- vColClasses[i]
		
		if(vc == "integer")
		{
			S[,i] <- as.integer(S[,i])
		}
		else if(vc == "numeric")
		{
			S[,i] <- as.numeric(S[,i])
		}
		else if(vc == "logical")
		{
			S[,i] <- as.logical(S[,i])
		}
		else if(vc == "character")
		{
			S[,i] <- as.character(S[,i])
		}
		else if(vc == "Date")
		{
			S[,i] <- as.Date(S[,i], format="%Y%m%d")
		}
		else
		{
			S[,i] <- as.character(S[,i])
		}
	}

	return(S)
}