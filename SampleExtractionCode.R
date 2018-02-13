library(tcltk)
library(sqldf)
library(dplyr)
library(arules)
library(rpart)
library(DBI) 
library(RSQLite)
library(ggplot2)
library(plyr)

## Download database.sqlite from 
##https://www.kaggle.com/hhs/health-insurance-marketplace/downloads/health-insurance-marketplace-release-2016-01-20-15-52-37.zip
##FYI: The zip file is 701 mb. database.sqlite is 4GB. 

dbdriver = dbDriver("SQLite")
connect  = dbConnect(dbdriver, dbname = "../Documents/database.sqlite")

dbListTables(connect)

rate <- dbGetQuery(connect,"select * from Rate")
rate <- dbGetQuery(connect,"select * from Rate")
BenefitsCost<-dbGetQuery(connect,"select * from BenefitsCostSharing")
PlannAtt<-dbGetQuery(connect,"select * from PlanAttributes")
Network<-dbGetQuery(connect,"select* from Network")


ratetest <- rate
colnames(ratetest)

#Kept the following fields; BusinessYear, StateCode,PlanId,RatingAreaId,Tobacco,Age,IndividualRate,IndividualTobaccoRate,
#Couple,PrimarySubscriberAndOneDependent, PrimarySubscriberAndTwoDependents,"PrimarySubscriberAndThreeOrMoreDependents"
#CoupleAndOneDependent,CoupleAndTwoDependents                   
#CoupleAndThreeOrMoreDependents  
#Can be adjusted as we see fit 
ratetest1 <- ratetest[,c(-3,-4,-5,-6,-7,-8,-9,-10,-24)]

ratetest1<- ratetest1[!duplicated(ratetest1),]

BenefitsCostTest<-BenefitsCost

BenefitsCostTest1 <- BenefitsCostTest[,c(2,30,29,1,3,6,13,14,17)]

PlannAtt<-dbGetQuery(connect,"select * from PlanAttributes")
#IsNoticeRequiredForPregnancy,IsReferralRequiredForSpecialist,DiseaseManagementProgramsOffered,OutOfCountryCoverage,OutOfServiceAreaCoverage
#StandardComponentId
PlannAttTest<-PlannAtt
PlannAttTest1<-PlannAttTest[,c(135,5,62,63,45,115,108,110,134)]

planatts<-sqldf("select businessyear, statecode, standardcomponentid, count(*) as countofAtts from PlannAttTest1
+ group by businessyear, statecode, standardcomponentid
+ order by businessyear, statecode")

planatts2<-sqldf("select businessyear, statecode, standardcomponentid, IsNoticeRequiredForPregnancy as pregnotice from PlannAttTest1
                 
                  group by businessyear,statecode, standardcomponentid
                  order by businessyear, statecode")

planatts3<-sqldf("select businessyear, statecode, standardcomponentid, IsReferralRequiredForSpecialist as refneeded from PlannAttTest1
                  
                  group by businessyear,statecode, standardcomponentid
                  order by businessyear, statecode")

planatts4<-sqldf("select businessyear, statecode, standardcomponentid, DiseaseManagementProgramsOffered as diseasemngmnt from PlannAttTest1
                  group by businessyear,statecode, standardcomponentid
                  order by businessyear, statecode")

planatts5<-sqldf("select businessyear, statecode, standardcomponentid, OutOfCountryCoverage as outofcountry from PlannAttTest1
                
                  group by businessyear,statecode, standardcomponentid
                  order by businessyear, statecode")
planatts6<-sqldf("select businessyear, statecode, standardcomponentid, OutOfServiceAreaCoverage as outofservcearea from PlannAttTest1
                  
                  group by businessyear,statecode, standardcomponentid
                  order by businessyear, statecode")


planbenefits <- sqldf("select businessyear,statecode, standardcomponentid, count(*) as benefits from BenefitsCostTest1 
                       group by businessyear,statecode, standardcomponentid 
                       order by businessyear,statecode ")
colnames(planbenefits)

planbenefits2 <- sqldf("select businessyear,statecode, standardcomponentid, ISCovered as iscovered from BenefitsCostTest1 
                       
                       group by businessyear,statecode, standardcomponentid 
                       order by businessyear,statecode ")
head(planbenefits2)

unique(BenefitsCostTest1$IsEHB)
planbenefits3 <- sqldf("select businessyear,statecode, standardcomponentid, IsEHB as IsEHB from BenefitsCostTest1 
                       
                       group by businessyear,statecode, standardcomponentid 
                       order by businessyear,statecode ")
head(planbenefits3)

unique(BenefitsCostTest1$IsStateMandate)
planbenefits4 <- sqldf("select businessyear,statecode, standardcomponentid, IsStateMandate as IsStatemandate from BenefitsCostTest1 
                       group by businessyear,statecode, standardcomponentid 
                       order by businessyear,statecode ")
head(planbenefits4)

unique(Network$NetworkName)
networkinfo<-sqldf("select businessyear,statecode, NetworkName as NetworkName from Network
                    group by businessyear, statecode
                    order by businessyear, statecode ")
networkinfo2<-sqldf("select businessyear, statecode, MarketCoverage as MarketCoverage from Network 
                      group by businessyear, statecode
                      order by businessyear, statecode")

#Merge into one dataframe


final <- sqldf("select r.businessyear, r.statecode, r.planid, r.age, r.individualrate, pb.benefits
                from ratetest1 r left outer join planbenefits pb on  
                                         r.businessyear = pb.businessyear and
                                         r.statecode    = pb.statecode and 
                                         r.planid       = pb.standardcomponentid 
               ")

final <- sqldf("select r.businessyear, r.statecode, r.planid, r.age, r.individualrate, r.benefits, pb2.IScovered 
                  from final r left outer join planbenefits2 pb2 on
                                          r.businessyear = pb2.businessyear and
                                          r.statecode    = pb2.statecode and 
                                          r.planid       = pb2.standardcomponentid  ")

final <- sqldf("select r.businessyear, r.statecode, r.planid, r.age, r.individualrate, r.benefits,r.IScovered, pb2.IsEHB 
                  from final r left outer join planbenefits3 pb2 on
                                          r.businessyear = pb2.businessyear and
                                          r.statecode    = pb2.statecode and 
                                          r.planid       = pb2.standardcomponentid  ")

final <- sqldf("select r.businessyear, r.statecode, r.planid, r.age, r.individualrate, r.benefits, r.IScovered, r.IsEHB, pb2.IsStatemandate
                  from final r left outer join planbenefits4 pb2 on
                                                                  r.businessyear = pb2.businessyear and
                                                                  r.statecode    = pb2.statecode and 
                                                                  r.planid       = pb2.standardcomponentid  ")


final <- sqldf("select r.businessyear, r.statecode, r.planid, r.age, r.individualrate,r.benefits, r.IScovered, r.IsEHB, r.IsStatemandate,pb2.countofAtts
                from final r left outer join planatts pb2 on   
                                         r.businessyear = pb2.businessyear and
                                         r.statecode    = pb2.statecode and 
                                         r.planid       = pb2.standardcomponentid 
               ")

final <- sqldf("select r.businessyear, r.statecode, r.planid, r.age, r.individualrate,r.benefits, r.IScovered, r.IsEHB, r.IsStatemandate,r.countofAtts, pb2.pregnotice
                from final r left outer join planatts2 pb2 on  
                                         r.businessyear = pb2.businessyear and
                                         r.statecode    = pb2.statecode and 
                                         r.planid       = pb2.standardcomponentid 
               ")
final <- sqldf("select r.businessyear, r.statecode, r.planid, r.age, r.individualrate,r.benefits, r.IScovered, r.IsEHB, r.IsStatemandate,r.countofAtts, r.pregnotice, pb2.refneeded
                from final r left outer join planatts3 pb2 on  
                                         r.businessyear = pb2.businessyear and
                                         r.statecode    = pb2.statecode and 
                                         r.planid       = pb2.standardcomponentid 
               ")
final <- sqldf("select r.businessyear, r.statecode, r.planid, r.age, r.individualrate,r.benefits, r.IScovered, r.IsEHB, r.IsStatemandate,r.countofAtts, r.pregnotice, r.refneeded, pb2.diseasemngmnt
                from final r left outer join planatts4 pb2 on  
                                         r.businessyear = pb2.businessyear and
                                         r.statecode    = pb2.statecode and 
                                         r.planid       = pb2.standardcomponentid 
               ")

final <- sqldf("select r.businessyear, r.statecode, r.planid, r.age, r.individualrate,r.benefits, r.IScovered, r.IsEHB, r.IsStatemandate,r.countofAtts, r.pregnotice, r.refneeded, r.diseasemngmnt, pb2.outofcountry
                from final r left outer join planatts5 pb2 on  
                                         r.businessyear = pb2.businessyear and
                                         r.statecode    = pb2.statecode and 
                                         r.planid       = pb2.standardcomponentid 
               ")

final <- sqldf("select r.businessyear, r.statecode, r.planid, r.age, r.individualrate,r.benefits, r.IScovered, r.IsEHB, r.IsStatemandate,r.countofAtts, r.pregnotice, r.refneeded, r.diseasemngmnt, r.outofcountry, pb2.outofservcearea
                from final r left outer join planatts6 pb2 on  
                                         r.businessyear = pb2.businessyear and
                                         r.statecode    = pb2.statecode and 
                                         r.planid       = pb2.standardcomponentid
                ")
final <- sqldf("select r.businessyear, r.statecode, r.planid, r.age, r.individualrate,r.benefits, r.IScovered, r.IsEHB, r.IsStatemandate,r.countofAtts, r.pregnotice, r.refneeded, r.diseasemngmnt, r.outofcountry, r.outofservcearea, pb2.NetworkName
                from final r left outer join networkinfo pb2 on  
                                         r.businessyear = pb2.businessyear and
                                         r.statecode    = pb2.statecode 
                ")
final <- sqldf("select r.businessyear, r.statecode, r.planid, r.age, r.individualrate,r.benefits, r.IScovered, r.IsEHB, r.IsStatemandate,r.countofAtts, r.pregnotice, r.refneeded, r.diseasemngmnt, r.outofcountry, r.outofservcearea,r.NetworkName,pb2.MarketCoverage  
                from final r left outer join networkinfo2 pb2 on  
                                         r.businessyear = pb2.businessyear and
                                         r.statecode    = pb2.statecode                        
                ")
final[is.na(final)] <- 0
colnames(final)

finl <- final[!duplicated(final),]
finl<-finl[complete.cases(finl),]

final[is.na(final)] <- 0
finl <- final[!duplicated(final),]
finl<-finl[complete.cases(finl),]
write.csv(finl,"MainDatabaseSample.csv")
