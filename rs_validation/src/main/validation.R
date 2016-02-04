library(reshape2)
library(recommenderlab)
library(dplyr)


get_sampled_matrix = function(mt, pr){
	n = length(mt)
	sample_index = sample(n, n*pr, replace = FALSE)
	mt[sample_index] =  NA
	return(mt)
}

get_rmse = function(ma, mb){
	sqrt(sum((ma-mb)^2)/length(ma))
}


get_prediction_error = function(p){
	# data preparation
	filename = paste("/home/max/codeZ/songchun/new_matrix.csv")
	df = read.csv(filename)
	original_rating_matrix = as.matrix(df)
	incomplete_rating_matrix = get_sampled_matrix(original_rating_matrix, p)  # p is the ratio of data points to delete
	
	# recommendation
	ratingMatrix = as(incomplete_rating_matrix, "realRatingMatrix")
	#colnames(ratingMatrix) = paste("M", 1:ncol(incomplete_rating_matrix), sep="")
	#recommModel = Recommender(ratingMatrix[1:100,], method = "IBCF")
	recommModel = Recommender(ratingMatrix, method = "UBCF")
	predictMatrix = predict(recommModel, newdata = ratingMatrix, type = "ratings")

	# evaluating sample + recommendation against origianl rating matrix
	new_rating_matrix = pmin(as(predictMatrix, "matrix"), as(ratingMatrix, "matrix"), na.rm=TRUE) # merge incomplete matrix with predicted matrix

	rmse = get_rmse(new_rating_matrix, original_rating_matrix)
	
	# compute AUC
	# for (user in original_rating_matrix)
	overall_AUC = 0
	for (row in (1:nrow(original_rating_matrix))) {
	  user = sort(original_rating_matrix[row, ]) # row is the list of original ratings for that user
	  #cat(user, "\n")
	  #we assume row are the same in both matrix (i.e row 1 = user 1 in both), and they have the same length.
	  pred_user = sort(new_rating_matrix[row, ]) # row is the list of predicted ratings for that user
	  #cat(pred_user, "\n")
	  user_AUC = 0
	  for (product in 1:length(user)) {
	    p_name = names(user[product])
	    #cat(p_name, "\n")
	    p_real_rank = match(p_name, names(user))
	    p_pred_rank = match(p_name, names(pred_user))
	    #cat(p_name, " real rank: ", p_real_rank, "\n")
	    #cat(p_name, "pred rank: ", p_pred_rank, "\n")
	    p_AUC = p_pred_rank - 1
	    #handle case where both products are ranked first
	    if ( (p_real_rank == 1 && p_pred_rank == 1 ) || p_pred_rank == 1) {
	      user_AUC =  user_AUC + 1
	    } else {
	      for (prediction in 1:(p_pred_rank - 1)) {
	        prediction_name = names(pred_user[prediction])
	        prediction_real_rank = match(prediction_name, names(user))
	        #cat("predicted rank under study: ", prediction, " real rank for that item: ", prediction_real_rank, "\n")
	        if (prediction_real_rank > p_real_rank && abs(pred_user[prediction] - user[product]) > 0.15) {
	          p_AUC = p_AUC - 1
	        }
	      }
	      user_AUC = user_AUC + (p_AUC / (p_pred_rank - 1)) # add up the products AUC for that user
	    }
	  }
	  
	  overall_AUC = overall_AUC + (user_AUC / dim(original_rating_matrix)[2]) #increment the overall AUC with the mean AUC for the current user
	}
	
	overall_AUC = overall_AUC / dim(original_rating_matrix)[1] #average the AUC over the number of users
	
	#write.table(new_rating_matrix, file=paste("10_30_score/predicted/", p, "/",file, "_", p, sep=''), sep=' ', col.names = F, row.names = F)

	return(overall_AUC)
}

# ================= the impact of pr =============================== #
#pr = seq(0, 1, by=0.01)
#all_error = sapply(pr, get_prediction_error_single_file)
#plot(pr, all_error, type='o', xlab="Portion of Data Points To Remove", ylab="Normalized RMSE")
#
# ================= for each population files ====================== #
#
#files = list.files(path = "10_30_score/original", pattern = "score*", full.names=FALSE) 
#numFiles = length(files)
#rmses = rep(0, numFiles)
#for (i in 1:numFiles){
#	rmses[i] = get_prediction_error(files[i], 0.75)
#}
#plot(rmses, type='o')

sparsity <- c(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9)
#sparsity <- c(0.1)

for (s in sparsity) {
  print(get_prediction_error(s))
}

