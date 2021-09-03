library(doParallel)
numCores <- detectCores()/4
numCores
cl <- makeCluster(numCores)
registerDoParallel(cl,cores = numCores, nocompile = FALSE)

library(tidymodels)
library(parsnip)
library(modeltime.h2o)
library(h2o)
library(dplyr)
library(timetk)
library(lubridate)
library(tidyr)
#library(dtplyr)
library(purrr)
#install.packages("modelr")
library(doMC)
library(multidplyr)
#registerDoMC(cores = 6)
library(h2o)
library(data.table)
library(doParallel)

#cluster_library(cl, c("dplyr","tidymodels","workflowsets","recipes","h2o","modeltime.h2o","parsnip","timetk"))
setwd("~/Documents/serviceRcx")
files <- list.files("./train/")
newfiles <- list.files("./test/")
all_dat <- tibble() #read.csv(paste("./train/",files[1],sep = ""))
#dat <- read.csv(paste("./train/",files[1],sep = ""))
mods <- modeltime_table()
tp <- tibble()
h2o.init(max_mem_size = "32g")
foreach(i = files[1:3]) %do% {
 
  setwd("~/Documents/serviceRcx")
all_dat <-  fread(paste("./train/",i,sep = ""))
  all_dat$Datetime <- ymd_hms(all_dat$time)
  # %>% group_by(ID) %>% partition(cluster) 
  
  all_dat <- all_dat %>% filter(., Temp > -100)# %>% select(Temp,eload,weekhour,ID)
  all_dat <-all_dat %>% filter(., eload > 0)
  recipe_spec <- all_dat %>% recipe(eload ~ ., data = .) %>% step_timeseries_signature(Datetime) %>% step_corr(Temp) #%>% step_normalize(eload)
 
  
  all_tbl <- bake(prep(recipe_spec), all_dat)
  all_tbl$Datetime <- as.Date(all_tbl$time)
  all_tbl$time <- as.Date(all_tbl$time)
   splits <- initial_split(all_tbl)
  train_tbl <- training(splits)
  test_tbl <- testing(splits)

 
  
  model_spec <- automl_reg(mode = 'regression') %>%
    set_engine(
      engine = 'h2o',
      max_runtime_secs = 90,
      max_runtime_secs_per_model = 30,
      #project_name = 'project_01',
      #keep_cross_validation_predictions = TRUE,
      nfolds = 5,
      max_models = 20,
      exclude_algos = c("DeepLearning","GLM","DRF"),
      stopping_metric = "mse",
      stopping_rounds =3,
      stopping_tolerance = 0.05,
      seed = 786)
 #h2o.init(nthreads = 12, max_mem_size = "24g")
 
  model_fitted <- model_spec %>%
    fit(eload ~.,data = train_tbl) 
 model_fitted <-modeltime_calibrate(model_fitted,test_tbl, id = "ID")
  path <- paste("models1",as.character(unique(all_dat$ID)), sep = "")
  mods <-mods %>%bind_rows(model_fitted)
  #setwd("~/Documents/serviceRCX/models1")
  save_h2o_model(model_fitted,path = path)
  #all_dat <- all_dat %>% mutate(Datetime=map(time,lubridate::ymd_hms))
 
}
#h2o.init(max_mem_size = "32g")
#mods <- modeltime_table()
#foreach(i = modl.ist) %do% {
#  mod <- load_h2o_model(i)
  
#  mods <- mods %>% add_modeltime_model(mod)
  #all_dat <- all_dat %>% mutate(Datetime=map(time,lubridate::ymd_hms))
  
#}

#all_dat_new <- foreach(i = newfiles) %dopar% {
 #fread(paste("./test/",i,sep = ""))
  #all_dat <- all_dat %>% mutate(Datetime=map(
#}
#all_dat_new <- bind_rows(all_dat_new)
#all_dat_new$Datetime <- ymd_hms(all_dat_new$time)
#recipe_spec_new <- all_dat_new %>% recipe() %>% step_timeseries_signature(Datetime) %>% step_corr(Temp) #%>% step_normalize(eload)
#all_tbl_new <- bake(prep(recipe_spec), all_dat_new)
#all_tbl_new$Datetime <- as.Date(all_tbl_new$Datetime)
#preds <- (mods,all_tbl_new)
#mods <-  modeltime_calibrate(mods,new_data = all_tbl,id = "ID")
h2o.removeAll()
h2o.shutdown(prompt = FALSE)
closeAllConnections()
stopCluster()
stopImplicitCluster()

