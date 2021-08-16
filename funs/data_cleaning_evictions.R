library(DBI)
library(stringr)
library(dplyr)
library(ggplot2)
library(fastDummies)
library(tidyr)
library(rstudioapi)

#Set today
today <- Sys.Date()
today <- format(today, format = "%Y%m%d")
#today <- "20210812"

connectDB <- function() {
  db_name20 <- paste(today, "_ojdevictions_2020", sep = "")
  db_name21 <- paste(today, "_ojdevictions_2021", sep = "")
  
  con20 <- dbConnect(RPostgres::Postgres(),dbname = db_name20,
                   host = 'localhost',
                   port = '5432',
                   user = 'postgres',
                   password = 'admin')
  
  con21 <- dbConnect(RPostgres::Postgres(),dbname = db_name21,
                   host = 'localhost',
                   port = '5432',
                   user = 'postgres',
                   password = 'admin')
  return(c(con20, con21))
}
getCaseOverviews <- function() {
  con20 <- connectDB()[[1]]
  con21 <- connectDB()[[2]]
  
  query20 <- dbSendQuery(con20, 'SELECT * FROM "case-overviews"')
  case_overviews20 <- dbFetch(query20)
  dbClearResult(query20)
  
  query21 <- dbSendQuery(con21, 'SELECT * FROM "case-overviews"')
  case_overviews21 <- dbFetch(query21)
  dbClearResult(query21)
  
  case_overviews <- bind_rows(case_overviews20, case_overviews21)
  case_overviews$style <- str_replace_all(case_overviews$style, "\n", " ")
  return(case_overviews)
}
getCaseParties <- function() {
  con20 <- connectDB()[[1]]
  con21 <- connectDB()[[2]]
  
  query20 <- dbSendQuery(con20, 'SELECT * FROM "case-parties"')
  case_parties20 <- dbFetch(query20)
  dbClearResult(query20)
  
  query21 <- dbSendQuery(con21, 'SELECT * FROM "case-parties"')
  case_parties21 <- dbFetch(query21)
  dbClearResult(query21)
  
  case_parties <- bind_rows(case_parties20, case_parties21)

  return(case_parties)
}
getEvents <- function() {
  con20 <- connectDB()[[1]]
  con21 <- connectDB()[[2]]
  
  query20 <- dbSendQuery(con20, 'SELECT * FROM "events"')
  events20 <- dbFetch(query20)
  dbClearResult(query20)
  
  query21 <- dbSendQuery(con21, 'SELECT * FROM "events"')
  events21 <- dbFetch(query21)
  dbClearResult(query21)
  
  events <- bind_rows(events20, events21)
  return(events)
}
getFiles <- function() {
  con20 <- connectDB()[[1]]
  con21 <- connectDB()[[2]]
  
  query20 <- dbSendQuery(con20, 'SELECT * FROM "files"')
  files20 <- dbFetch(query20)
  dbClearResult(query20)
  
  query21 <- dbSendQuery(con21, 'SELECT * FROM "files"')
  files21 <- dbFetch(query21)
  dbClearResult(query21)
  
  files <- bind_rows(files20, files21)
  return(files)

}
getJudgments <- function() {
  con20 <- connectDB()[[1]]
  con21 <- connectDB()[[2]]
  
  query20 <- dbSendQuery(con20, 'SELECT * FROM "judgments"')
  judgments20 <- dbFetch(query20)
  dbClearResult(query20)
  
  query21 <- dbSendQuery(con21, 'SELECT * FROM "judgments"')
  judgments21 <- dbFetch(query21)
  dbClearResult(query21)
  
  judgments <- bind_rows(judgments20, judgments21)
  return(judgments)
}
getLawyers <- function() {
  con20 <- connectDB()[[1]]
  con21 <- connectDB()[[2]]
  
  query20 <- dbSendQuery(con20, 'SELECT * FROM "lawyers"')
  lawyers20 <- dbFetch(query20)
  dbClearResult(query20)
  
  query21 <- dbSendQuery(con21, 'SELECT * FROM "lawyers"')
  lawyers21 <- dbFetch(query21)
  dbClearResult(query21)
  
  lawyers <- bind_rows(lawyers20, lawyers21)
  return(lawyers)
}
getDefendantInfo <- function() {
  getCaseParties() %>%
    filter(party_side == "Defendant") %>%
    group_by(case_code) %>%
    summarize(defendant_names = paste(name, collapse = "; "),
              defendant_addr = paste(unique(addr), collapse = "; ")) %>% 
    return()
}
joinDefendantInfo <- function() {
  getCaseOverviews() %>%
    full_join(getDefendantInfo(), by = 'case_code')
    #full_join(files %>% select(case_code, path), by = 'case_code')
}

# exportFlatFileCSV <- function() {
#   setwd(paste("C:/AUTO_SCRAPE_OJD/Court_Documents_", today, sep = ""))
#   write.csv(joinDefendantInfo(), "case_overviews_flat.csv")
# }



