library(tidyverse)

data <- read.csv("data/individual_records/disease_classification_demographics.csv")

#find data with inconsistencies in sex or race and replace with NA
data <- data %>% mutate(sex = ifelse(sex %in% c("M,U","F,U","F,M","F,M,U"), NA, sex)) %>%
                 mutate(race_ethnicity_code = ifelse(grepl(",|0", race_ethnicity_code), NA, race_ethnicity_code))

write.csv(data, "data/individual_records/disease_classification_demographics_with_nas.csv")