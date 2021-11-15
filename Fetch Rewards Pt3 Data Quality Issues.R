library(jsonlite)
library(dplyr)
library(tidyr)


#Reading files
receipts_file <- ("C:\\Users\\18572\\OneDrive\\Desktop\\Fetch Rewards\\JSON_Data\\receipts.json")

receipts_df <- jsonlite::stream_in(file(receipts_file))

users_file <- ("C:\\Users\\18572\\OneDrive\\Desktop\\Fetch Rewards\\JSON_Data\\users.json") 

users_df <- jsonlite::stream_in(file(users_file))

brands_file <- ("C:\\Users\\18572\\OneDrive\\Desktop\\Fetch Rewards\\JSON_Data\\brands.json")

brands_df <- jsonlite::stream_in(file(brands_file))

#Manipulation
receipts_df <- receipts_df %>%
  mutate(RecieptID = `_id`$`$oid`)

users_df <- users_df %>%
  mutate(userID = `_id`$`$oid`)

#Exploration

#Can barcode be used as Primary Key in brands_df?
#1160 barcodes detected. Total rows = 1167.
length(unique(brands_df$barcode))

#No NAs detected in barcode
brands_df[is.na(brands_df$barcode),]

#7 barcodes duplicated
nrow(brands_df[duplicated(brands_df$barcode),])
sum(duplicated(brands_df$barcode))

#1167 unique ids
length(unique(brands_df["_id"][,1]$`$oid`))

#Can user_id be used as Primary Key in users_df?
length(unique(users_df["_id"][,1]$`$oid`))

#283 users duplicated
nrow(users_df[duplicated(users_df["_id"][,1]$`$oid`),])
sum(duplicated(users_df["_id"][,1]$`$oid`))

#Outliser detection
#bonusPointsEarned
summary(receipts_df$bonusPointsEarned)
iqr = quantile(receipts_df$bonusPointsEarned, 0.75, na.rm = TRUE)[[1]] - quantile(receipts_df$bonusPointsEarned, 0.25, na.rm = TRUE)[[1]]
nrow(receipts_df[receipts_df$bonusPointsEarned > 1.5*iqr,])

#pointsEarned is character data type
summary(receipts_df$pointsEarned)

#purchasedItemCount
summary(receipts_df$purchasedItemCount)
iqr = quantile(receipts_df$purchasedItemCount, 0.75, na.rm = TRUE)[[1]] - quantile(receipts_df$purchasedItemCount, 0.25, na.rm = TRUE)[[1]]
nrow(receipts_df[receipts_df$purchasedItemCount > 1.5*iqr,])

#totalSpent is character data type
summary(receipts_df$totalSpent)

#Joining users and receipts to validate Foreign Key userID
unique_users_df <- unique(users_df)

user_receipts <- merge(x = receipts_df, y = unique_users_df, by.x = "userId", by.y = "userID")

#Varying columns in rewardsReceiptItemLists
column_names <- as.list(colnames(receipts_df$rewardsReceiptItemList[[1]]))
for (i in 2:nrow(receipts_df)){
  column_names <- append(column_names, as.list(colnames(receipts_df$rewardsReceiptItemList[[i]])))
}

#Getting column names that exist for all receipts
itemList_column_names <- unlist(unique(column_names))

#Creating empty data frame
rewardsReceiptsItemLists_df <- data.frame(matrix(ncol = length(itemList_column_names)))
colnames(rewardsReceiptsItemLists_df) <- itemList_column_names

#Generating a rewards reciepts item list data frame to query against brand_df
for (i in 1:nrow(receipts_df)){
  R_id <- receipts_df$RecieptID[i]
  temp <- mutate(as.data.frame(receipts_df$rewardsReceiptItemList[[i]]), R_id)
  rewardsReceiptsItemLists_df <- bind_rows(rewardsReceiptsItemLists_df, temp)
}

#Joining receipts and brands to validate Foreign Key barcode and brandcode
#brand_receipts <- merge(x = rewardsReceiptsItemLists_df, y = brands_df, by.x = "barcode", by.y = "barcode")
brand_receipts <- merge(x = rewardsReceiptsItemLists_df, y = brands_df, by.x = c("barcode","brandCode"), by.y = c("barcode","brandCode"))

#checking to see if there exists other unique relationships between attribute
intersect(rewardsReceiptsItemLists_df$rewardsProductPartnerId, brands_df["cpg"][,1]$`$id`)

#checking to see if there exists other unique relationships between attribute
intersect(rewardsReceiptsItemLists_df$pointsPayerId, brands_df["cpg"][,1]$`$id`)
