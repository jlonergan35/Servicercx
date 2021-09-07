library(modeltime.h2o)
library(doParallel)
numCores <- detectCores()/4
numCores
cl <- makeCluster(numCores)
parallel_start(cl)
#registerDoParallel(cl,cores = numCores, nocompile = FALSE)

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
forcs <- tibble()
tp <- tibble()
h2o.init(max_mem_size = "32g")
foreach(i = 1:367) %do% {
 
  setwd("~/Documents/serviceRcx")
all_dat <-  fread(paste("./train/",files[i],sep = ""))
  all_dat$Datetime <- ymd_hms(all_dat$time)
  # %>% group_by(ID) %>% partition(cluster) 
  
  all_dat <- all_dat %>% filter(., Temp > -100)# %>% select(Temp,eload,weekhour,ID)
  #all_dat <-all_dat %>% filter(., eload > 0)
  recipe_spec <- all_dat %>% recipe(eload ~ ., data = .) %>% step_timeseries_signature(Datetime) %>% step_corr(Temp) #%>% step_normalize(eload)
 
  
  all_tbl <- bake(prep(recipe_spec), all_dat)
  all_tbl$Datetime <- as.Date(all_tbl$time)
  all_tbl$time <- as.Date(all_tbl$time)
   splits <- initial_split(all_tbl)
  train_tbl <- training(splits)
  test_tbl <- testing(splits)

  path <- as.character(unique(all_dat$ID))
  
  model_spec <- automl_reg(mode = 'regression') %>%
    set_engine(
      engine = 'h2o',
      max_runtime_secs = 300,
      max_runtime_secs_per_model = 60,
      #project_name = 'project_01',
      #keep_cross_validation_predictions = TRUE,
      nfolds = 5,
      max_models = 30,
      exclude_algos = c("DeepLearning","GLM","DRF"),
      stopping_metric = "mse",
      stopping_rounds =3,
      stopping_tolerance = 0.01,
      seed = 786)
 #h2o.init(nthreads = 12, max_mem_size = "24g")
 
  model_fitted <- model_spec %>%
    fit(eload ~.,data = train_tbl)
  model_fitted$.model_id <- newfiles[i]
  
  #setwd("~/Documents/serviceRcx/models")
  save_h2o_model(model_fitted,path =paste("./models/",path, sep = ""),overwrite = TRUE)
  model_fitted <-modeltime_calibrate(model_fitted,test_tbl, id = "ID")
  
  newdat1 <- fread(paste("./test/",newfiles[i],sep = ""))
  newdat1$Datetime <- ymd_hms(newdat1$time)
  #newdat1$weekhour <- (lubridate::wday(mdy_hm(newdat1$time))-1)*24+lubridate::hour(mdy_hm(newdat1$time))
  new_tbl <- bake(prep(recipe_spec), newdat1)
  new_tbl$Datetime <- as.Date(new_tbl$Datetime)
  new_tbl$time <- as.Date(new_tbl$time)
 forcs <- forcs %>% bind_rows(modeltime_forecast(model_fitted,new_tbl))
  mods <-mods %>%bind_rows(model_fitted)
  
 
 # setwd("~/Documents/s vcerviceRcx/models")
  #save_h2o_model(model_fitted,path = path)

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

mods$.model_id <- as.numeric(list.files("./models/"))
flies <- list.files("./test/")
post <- tibble()
foreach(i = flies) %do% {post <- post %>% bind_rows(fread(paste("./test/",i,sep = "")))}
post$KWH <- forcs$.value
evo <- post %>% select(time,KWH,ID)
write.csv(evo,"evo.csv")

h2o.removeAll()
h2o.shutdown(prompt = FALSE)
closeAllConnections()
#stopCluster()
stopImplicitCluster()
#parallel_stop(cl)
