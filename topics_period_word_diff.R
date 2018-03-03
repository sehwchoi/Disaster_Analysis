#install.packages("wordcloud")
#install.packages("RColorBrewer")
#install.packages("readr")
library(RColorBrewer)
library(wordcloud)
dev.off()
setwd("/Users/stellachoi/Box Sync/research_work/disaster_social/twitter_analysis/data/topic_models/topics")

topics<-read.csv("319_unique_af_topics.csv", header=TRUE)

for (i in 0:8) {
  pal2 <- brewer.pal(8,"Dark2")
  #View(topics)
  print(i)
  file_name <- paste("wordcloud_af_", i,".png", sep = "") 
  print(file_name)
  png(file_name, width=3,height=3, units='in', res=300)
  wordcloud(topics[topics$topic==i,]$word,topics[topics$topic==i,]$weight,random.color=FALSE,colors=pal2)
  dev.off()
}

