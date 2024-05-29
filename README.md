# Clustering Project
## Description
This project processes a dataset of Instagram posts. It performs the following tasks:

Uses an embedding model to convert the content_text of each post into a vector.
Calculates the optimal number of clusters using the elbow method.
Divides posts into clusters using the sklearn KMeans classifier.
Assigns each cluster a name using three methods:
1. N-grams
2. TF-IDF
3. A combination of N-grams and TF-IDF

## Installation
clone the repository, install the required dependencies.
the script can be found in the url: "./analytics/clustering-scripts/clustering-by-caption/posts-clustering.ipynb"

## Configuration
To configure the script, you can find the global configuration settings at the top of the script:

* Clustering Parameters

  * <b>MIN_CLUSTERS:</b> The minimum number of clusters.
  * <b>MAX_CLUSTERS:</b> The maximum number of clusters.
* Column Names

  * <b>CAPTIONS_COLUMN_NAME:</b> The name of the content_text column inside the dataset . 
  * <b>EMBEDDINGS_COLUMN_NAME:</b> The name of the column that includes/will include (depends on <b>embed_posts_captions</b> flag) the posts' captions embeddings in the dataset..
* File Paths

  * <b>script_outputs_folder_url:</b> URL of the folder for script outputs.
  * <b>script_inputs_folder_url:</b> URL of the folder for script inputs.
  * <b>posts_data_file_url:</b> URL of the file containing posts data.
  * <b>embedded_posts_file_url:</b> URL of the file containing embedded posts data.
* Flags

  * <b>embed_posts_captions:</b> Set to True to embed posts' content_text.
  * <b>calculate_optimal_clusters_number:</b> Set to True to calculate the optimal number of clusters.
##### Note: If these flags are set to False, the script will use the output files from previous runs. Ensure to set the flags to True for the initial run to generate the necessary output files.
  * OPTIMAL_CLUSTERS_NUMBER
    -  Setting OPTIMAL_CLUSTERS_NUMBER to a positive number will cancel optimal cluster's number calculation process even if the calculate_optimal_clusters_number flag is turned on! and the chosen positive number will be used. 


## outputs
The script generates several files, which can be found in the output folder specified in the script configurations tab.
 1. <b>embedded_tweets.parquet</b> - A Parquet file that includes the original dataset with an additional embedding column.
 ##### Note: This file can be found in the inputs folder.
 2. <b>optimal-num-of-clusters-calculation-results.csv</b> - Includes all the results (WCSS calculated) from determining the optimal number of clusters.
 3. <b>optimal-clusters-number.csv</b> - Includes the optimal number of clusters
 4. <b>clusters-captions.parquet</b> - Includes all the posts' content text per cluster in a table.
 5. <b>n-gram-method-clusters-names.csv</b> - Includes cluster names produced using the n-gram method.
 6. <b>tf-idf-method-clusters-names.csv</b> - Includes cluster names produced using the tf-idf method.
 7. <b>n-gram-tfidf-method-clusters-names.csv</b> - Includes cluster names produced using the n-gram and tf-idf methods combined.
