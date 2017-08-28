
setwd("/Volumes/CLASS DISK/CSE 427S Cloud Computing with Big Data Applications/netflix_subset/netflix_subset/")
setwd("E:/CSE 427S Cloud Computing with Big Data Applications/netflix_subset/netflix_subset/")

train <- read.csv("TrainingRatings.txt", header = FALSE)
train1 <- train
train1$V1 <- factor(train$V1)
train1$V2 <- factor(train$V2)
str(train1)

test <- read.csv("TestingRatings.txt", header = FALSE)
test1 <- test
test1$V1 <- factor(test$V1)
test1$V2 <- factor(test$V2)
str(test1)

user_id <- unique(train$V1)
item_id <- unique(test$V1)


n <- length(unique(train$V1))
num <- matrix(, nrow = n, ncol = n)
for (i in 1:(n-1)){
  for (j in (i+1):n){
    x1 <- filter(train, V1 == user_id[i])
    x2 <- filter(train, V1 == user_id[j])
    xx <- intersect(x1$V2, x2$V2)
    num[i,j] <- length(xx)
  }
    
}

library(dplyr)
usertr <- group_by(train, V1)





