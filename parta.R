
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
