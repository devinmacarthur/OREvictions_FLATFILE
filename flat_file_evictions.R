source("funs/data_cleaning_evictions.R") 
library(fastDummies)

# setwd(dirname(getActiveDocumentContext()$path))

createJudgmentDummies <- function() {
  getJudgments() %>% 
    fastDummies::dummy_cols(select_columns = "case_type") %>% 
    group_by(case_code) %>% 
    summarise_if(is.numeric, sum, na.rm = TRUE) %>% 
    summarise(case_code = case_code,
              Judgment_General = ifelse(`case_type_Judgment - General` > 0 | 
                                          `case_type_Amended Judgment - General` > 0 |
                                          `case_type_Amended Judgment - Corrected General` > 0 | 
                                          `case_type_Judgment - Corrected General`, 1, 0),
              Judgment_Creates_Lien = ifelse(`case_type_Judgment - General Creates Lien` > 0 |
                                               `case_type_Judgment - Supplemental Creates Lien` > 0 |
                                               `case_type_Judgment - Limited Creates Lien` > 0 |
                                               `case_type_Amended Judgment - General Creates Lien` > 0 |
                                               `case_type_Judgment - General Dismissal Creates Lien` > 0 |
                                               `case_type_Amended Judgment - Corrected General Creates Lien` > 0 |
                                               `case_type_Judgment - Corrected General Creates Lien` > 0 |
                                               `case_type_Amended Judgment - Supplemental Creates Lien` > 0 |
                                               `case_type_Amended Judgment - Limited Creates Lien` > 0 |
                                               `case_type_Amended Judgment - Corrected Limited Creates Lien` > 0 |
                                               `case_type_Amended Judgment - Corrected Supplemental Creates Lien` > 0 |
                                               `case_type_Judgment - Corrected Supplemental Creates Lien` > 0 |
                                               `case_type_Judgment - Limited Dismissal Creates Lien` > 0, 1, 0),
              Judgment_Dismissal = ifelse(`case_type_Judgment - General Dismissal` > 0 |
                                            `case_type_Judgment - Limited Dismissal` > 0 |
                                            `case_type_Amended Judgment - General Dismissal` > 0 |
                                            `case_type_Amended Judgment - Limited Dismissal` > 0, 1, 0)) %>% 
    return()
}

addMoratoriumVars <- function() {
  getCaseOverviews() %>% 
    mutate(date2 = as.Date(getCaseOverviews()$date, "%m/%d/%Y"),
           Oregon_Moratorium = if_else(date2 >= as.Date('2020-3-22'), 1, 0),
           Multnomah_Moratorium = if_else(date2 >= as.Date('2020-3-17') & location == "Multnomah", 1, 0)) %>% 
    return()
}

getPlaintifNames <- function() {
  getCaseParties() %>% 
    filter(party_side == "Plaintiff") %>% 
    group_by(case_code) %>% 
    summarize(plaintiff_name = paste(name, collapse = "; ")) %>% 
    return()
}

getLawyersByParty <- function() {
  getCaseParties() %>%
    rename(party_name = name) %>% 
    select(case_code, party_name, party_side) %>% 
    right_join(getLawyers() %>% 
                 rename(lawyerName = name) %>%
                 select(case_code, party_name, lawyerName, status), by = c('case_code', 'party_name')) %>% 
    return()
}

getDefendantLawyers <- function() {
  getLawyersByParty() %>% 
    filter(party_side == "Defendant") %>% 
    group_by(case_code) %>% 
    summarize(party = paste(unique(party_name), collapse = "; "), 
              tenant_lawyer = paste(unique(lawyerName), collapse = "; ")) %>%
    return()
}

getPlaintiffLawyer <- function() {
  getLawyersByParty() %>% 
    filter(party_side == "Plaintiff") %>% 
    group_by(case_code) %>% 
    summarize(party = paste(unique(party_name), collapse = "; "), 
              landlord_lawyer = paste(unique(lawyerName), collapse = "; ")) %>% 
    return()
}

makeFTAvars <- function() {
  #makes Failure to Appear variable
  getEvents() %>% 
    filter(result == "FTA - Default" | result == "Failure to Appear") %>% 
    distinct(case_code) %>% 
    mutate(FTA = 1) %>% 
    return()
}

makeFTAFirstAppearance <- function(title, case_code, firstHearing, result) {
  getEvents() %>% 
    filter(grepl("hearing", title, ignore.case = TRUE)) %>%
    mutate(firstHearing = !duplicated(case_code)) %>% 
    select(firstHearing, case_code, title, result) %>% 
    filter(firstHearing == "TRUE") %>% 
    filter(grepl("FTA|Failure to Appear", result, ignore.case = TRUE)) %>% 
    mutate(FTAFirst = 1) %>% 
    return()
}

makeFlatFile <- function() {
  # makes final flat_file output
  addMoratoriumVars() %>% 
    select(case_code, style, date, Oregon_Moratorium, Multnomah_Moratorium, status, location) %>% 
    full_join(getPlaintifNames() %>% select(case_code, plaintiff_name), by = 'case_code') %>% 
    full_join(getDefendantInfo() %>% select(case_code, defendant_names, defendant_addr), by = 'case_code') %>% 
    full_join(createJudgmentDummies() %>% select(case_code, Judgment_General, 
                                       Judgment_Creates_Lien, 
                                       Judgment_Dismissal), by = 'case_code') %>% 
    full_join(getDefendantLawyers() %>% select(case_code, tenant_lawyer), by = 'case_code') %>% 
    full_join(getPlaintiffLawyer() %>% select(case_code, landlord_lawyer), by = 'case_code') %>% 
    full_join(makeFTAvars(), by = 'case_code') %>%
    full_join(makeFTAFirstAppearance() %>% select(case_code, FTAFirst), by = 'case_code') %>% 
    mutate(landlord_has_lawyer = ifelse(is.na(landlord_lawyer), 0, 1),
           tenant_has_lawyer = ifelse(is.na(tenant_lawyer), 0, 1),
           FTA = ifelse(is.na(FTA), 0, 1),
           FTAFirst = ifelse(is.na(FTAFirst), 0, 1),
           date = as.Date(date, "%m/%d/%Y"),
           month = as.Date(cut(date, breaks = "month")),
           # no_judgment = ifelse(status == "Closed" & judgment == "NULL", 1, 0),
           zip = word(defendant_addr, -1)) %>% 
    rename(case_name = style) %>% 
    return()
}

saveTablesRDS <- function() {
  saveRDS(makeFlatFile(), paste("output/full_scrape_", today, "/flat_file.rds", sep = ""))
  saveRDS(getCaseOverviews(), paste("output/full_scrape_", today, "/case_overviews.rds", sep = ""))
  saveRDS(getCaseParties(), paste("output/full_scrape_", today, "/case_parties.rds", sep = ""))
  saveRDS(getEvents(), paste("output/full_scrape_", today, "/events.rds", sep = ""))
  saveRDS(getJudgments(), paste("output/full_scrape_", today, "/judgments.rds", sep = ""))
  saveRDS(getLawyers(), paste("output/full_scrape_", today, "/lawyers.rds", sep = ""))
  saveRDS(getFiles(), paste("output/full_scrape_", today, "/files.rds", sep = ""))
  }


saveTablesCSV <- function(){
  write.csv(makeFlatFile(), paste("output/full_scrape_", today, "/flat_file.csv", sep = ""))
  write.csv(getCaseOverviews(), paste("output/full_scrape_", today, "/case_overviews.csv", sep = ""))
  write.csv(getCaseParties(), paste("output/full_scrape_", today, "/case_parties.csv", sep = ""))
  write.csv(getEvents(), paste("output/full_scrape_", today, "/events.csv", sep = ""))
  write.csv(getJudgments(), paste("output/full_scrape_", today, "/judgments.csv", sep = ""))
  write.csv(getLawyers(), paste("output/full_scrape_", today, "/lawyers.csv", sep = ""))
  write.csv(getFiles(), paste("output/full_scrape_", today, "/files.csv", sep = ""))
}

# set save directory
dir.create(paste("output/full_scrape", today, sep = "_"))
setwd(paste("output/full_scrape", today, sep = "_"))


dir.create(paste("output/csvfull_scrape", today, sep = "_"))
setwd(paste("output/csvfull_scrape", today, sep = "_"))

# Execute
saveTablesRDS()
saveTablesCSV()
