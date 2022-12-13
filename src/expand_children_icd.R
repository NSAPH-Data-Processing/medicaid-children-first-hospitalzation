library(icd)
library(jsonlite)

## generate comprehensive list of ICD desired
# read in desired ICD parents
icd_parents <- scan('icd_codes.txt', sep = ',', what = 'character')
icd_custom <- c()

# find all of the children
for (p in icd_parents) {
  icd_custom <- c(icd_custom, children(p))
}
# special case for E950-E959
# expands ICD codes that exists in start and end ranges
# see https://www.rdocumentation.org/packages/icd/versions/3.3/topics/expand_range
icd_range <- expand_range('E950', 'E959')

# append to icd_custom
icd_custom <- c(icd_custom, icd_range)

# write to json for python
outcomes <- list()
outcomes[["icd_cust_range"]] <- list()
outcomes[["icd_cust_range"]][["icd9_all"]] <- icd_custom

## classifying diseases by ICD
# motivation: Major depressive disorder 296.2x + 296.3x (can we group 311 with this?)
depression <- c(children('2962'), children('2963'))
outcomes[["icd_cust_range"]][["depression"]] <- depression

# motivation: Anxiety 300.0x to 300.29 + 300.4. 
anxiety <- c(children('3000'), '30029', '3004')
outcomes[["icd_cust_range"]][["anxiety"]] <- anxiety

# motivation: Disturbance of emotions specific to childhood (313.0-313.9) 
emotion_disturb <- c(children('313'))
outcomes[["icd_cust_range"]][["emotion_disturb"]] <- emotion_disturb

# motivation: Adolescence and adjustment reaction (309.00 to 309.98)
# only 30989 and 3099 are available. No 309.98.
adole_reaction <- c(children('309'))
outcomes[["icd_cust_range"]][["adole_reaction"]] <- adole_reaction

# motivation: Disturbance of conduct 312
disturb_conduct <- c(children('312'))
outcomes[["icd_cust_range"]][["disturb_conduct"]] <- disturb_conduct

# write_json(toJSON(outcomes), "icd_custom_Dec12_2022.json")
