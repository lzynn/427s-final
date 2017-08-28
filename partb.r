
setwd("E:/CSE 427S Cloud Computing with Big Data Applications/netflix_subset/netflix_subset/")

library(dplyr)

train <- read.csv("TrainingRatings.txt", header = FALSE)
test <- read.csv("TestingRatings.txt", header = FALSE)

item_id <- unique(train$V1)
item_id2 <- unique(test$V1)
user_id <- unique(train$V2)
user_id2 <- unique(test$V2)

n <- length(item_id)
n2 <- length(item_id2)
m <- length(user_id)
m2 <- length(user_id2)

user_tr <- list()
for (i in 1:(m)){
  user_tr[[i]] <- filter(train, V2 == user_id[i])
}

user_te <- list()
for (i in 1:(m2)){
  user_te[[i]] <- filter(test, V2 == user_id2[i])
}

item_tr <- list()
for (i in 1:(n)){
  item_tr[[i]] <- filter(train, V1 == item_id[i])
}
colum2 <- list()
for (i in 1:(n2)){
  item_te[[i]] <- filter(test, V1 == item_id2[i])
}

sum <- 0
for(i in 1:m2)
{
	id <- user_id2[i]
	len <- 0
	len1 <- 0
	len <- length(user_te[[i]]$V2)
	for(j in 1:m)
		if(user_id[j] == id)
			break;
	len1 <- length(user_tr[[j]]$V2)
	sum <- sum + len*len1
}


sum2 <- 0
for(i in 1:n2)
{
	id <- item_id2[i]
	len <- 0
	len1 <- 0
	len <- length(colum2[[i]]$V1)
	for(j in 1:m)
		if(item_id[j] == id)
			break;
	len1 <- length(colum[[j]]$V1)
	sum2 <- sum2 + len*len1
}


