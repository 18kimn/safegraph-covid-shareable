library(tidyverse)
library(tidycensus)
library(cwi)
library(sf)
library(furrr)
states <- c(unique(fips_codes$state)[1:51], "PR")

# Warning: this script takes a really long time to run and takes a lot of computing power
# plan(multiprocess) speeds this up by using multiple cores, but it's advised that users run
# a little bit at a time.

# It's helpful is users have access to a computer with relatively large memory. I ran this on the Yale 
# high-performance computing (HPC) service.

# "parallelizes" data cleaning
plan(multiprocess)

age_vars <- c(
  "B01001_002",
  "B01001_020",
  "B01001_021",
  "B01001_022",
  "B01001_023",
  "B01001_024",
  "B01001_025",
  "B01001_026",
  "B01001_044",
  "B01001_045",
  "B01001_046",
  "B01001_047",
  "B01001_048",
  "B01001_049"
)
agepop <- future_map_dfr(states,
                         function(x) {
                           get_acs(
                             "block group",
                             year = 2019,
                             variables = age_vars,
                             state = x,
                             output = "wide"
                           )
                         })
agepop %>%
  mutate(
    over65 = (
      B01001_020E + B01001_021E + B01001_022E + B01001_023E + B01001_024E +
        B01001_025E + B01001_044E + B01001_045E + B01001_046E + B01001_047E +
        B01001_048E + B01001_049E
    ) / (B01001_002E + B01001_026E)
  ) %>%
  select(blockgroup_fips = GEOID, over65) %>%
  saveRDS("scraped/over65.RDS")

racepop <- future_map_dfr(states,
                          function(x) {
                            message(x) #keep track of bottlenecks and errors
                            get_acs("block group",
                                    year = 2019,
                                    table = "B03002",
                                    state = x)
                          }) %>%
  label_acs() %>%
  select(blockgroup_fips = GEOID,
         population = estimate,
         race = label)

saveRDS(racepop, "scraped/racepop.RDS")

#income
income <- future_map_dfr(states,
                         function(x) {
                           message(x) #keep track of bottlenecks and errors
                           get_acs(
                             "block group",
                             year = 2019,
                             variables = "B19013_001",
                             state = x,
                             cache_table = T
                           )
                         }) %>%
  select(blockgroup_fips = GEOID, med_income = estimate)

saveRDS(income, "scraped/income.RDS")


#education
edu <- future_map_dfr(states,
                      function(x) {
                        message(x)
                        get_acs(
                          "block group",
                          year = 2019,
                          variables = c(
                            "B15002_002",
                            "B15002_015",
                            "B15002_016",
                            "B15002_017",
                            "B15002_018",
                            "B15002_019",
                            "B15002_032",
                            "B15002_033",
                            "B15002_034",
                            "B15002_035"
                          ),
                          state = x,
                          output = "wide"
                        )
                      }) %>%
  rename_all( ~ str_remove(., "B15002_")) %>%
  mutate(
    prop_college = (`015E` + `016E` + `017E` + `018E` + `032E` + `033E` + `034E` + `035E`) /
      (`002E` + `019E`)
  ) %>%
  select(blockgroup_fips = GEOID, prop_college)

saveRDS(edu, "scraped/edu.RDS")

#occupation


frontline_codes <-
  c(
    male_tot = "C24010_002",
    female_tot = "C24010_038",
    male_service = "C24010_019",
    female_service =  "C24010_055",
    male_prod = "C24010_034",
    female_prod = "C24010_070",
    male_health = "C24010_016",
    female_health = "C24010_052",
    male_sales = "C24010_027",
    female_sales = "C24010_063",
    male_nat = "C24010_033",
    female_nat = "C24010_069"
  )

occ <- future_map_dfr(states,
                      function(x) {
                        message(x)
                        get_acs(
                          "block group",
                          year = 2019,
                          variables = frontline_codes,
                          state = x,
                          output = "wide"
                        )
                      }) %>%
  select(-matches("M$")) %>%
  rename_all( ~ str_remove(., "E$")) %>% #some messy work to rename and select columns
  mutate(
    primary_front = (male_service + female_service + male_prod + female_prod) /
      (male_tot + female_tot),
    secondary_front = primary_front  + (
      male_health + female_health + male_sales + female_sales + male_nat + female_nat
    ) / (male_tot + female_tot)
  ) %>%
  select(blockgroup_fips = GEOID, primary_front, secondary_front)

saveRDS(occ, "scraped/occ.RDS")

# travel time to work -
travel_vars <- c(
  "0" = "002",
  "5" = "003",
  "10" = "004",
  "15" = "005",
  "20" = "006",
  "25" = "007",
  "30" = "008",
  "35" = "009",
  "40" = "010",
  "45" = "011",
  "60" = "012",
  "90" = "013"
)
travel_vars[] <- paste0("B08303_", travel_vars)

plan(multiprocess)
travel_times <- future_map_dfr(states,
                               function(x) {
                                 message(x)
                                 get_acs("block group",
                                         year = 2019,
                                         variables = travel_vars,
                                         state = x)
                               }) %>%
  filter(!is.na(estimate), estimate != 0) %>%
  mutate(travel_time = as.numeric(variable)) %>%
  group_by(GEOID) %>%
  summarize(avg_travel = weighted.mean(travel_time, estimate)) %>%
  select(blockgroup_fips = GEOID, avg_travel)
saveRDS(travel_times, "scraped/travel.RDS")

bg_shps <- readRDS("scraped/shps.RDS") %>%
  rmapshaper::ms_simplify(keep = 0.005)

areas <- bg_shps %>%
  mutate(area = units::set_units(st_area(.), km ^ 2)) %>%
  st_drop_geometry()
pop <- readRDS("scraped/pop.RDS")


pop_density <- pop %>%
  left_join(areas, by = c("blockgroup_fips" = "GEOID")) %>%
  mutate(density = population / area) %>%
  select(blockgroup_fips, density)
saveRDS(pop_density, "scraped/density.RDS")
