# Clustering Project
## Description
This internal company project processes a dataset of Instagram posts. It performs the following tasks:

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

  * MIN_CLUSTERS: The minimum number of clusters.
  * MAX_CLUSTERS: The maximum number of clusters.
* Column Names

  * CAPTIONS_COLUMN_NAME: The name of the content_text column inside the dataset . 
  * EMBEDDINGS_COLUMN_NAME: The name of the column that includes/will include (depends on <b>embed_posts_captions</b> flag) the posts' captions embeddings in the dataset. Default is "embedded_content_text".
* File Paths

  * script_outputs_folder_url: URL of the folder for script outputs. Default is './all-customers/clustering-script-outputs'.
  * script_inputs_folder_url: URL of the folder for script inputs. Default is './all-customers/clustering-inputs-data'.
  * posts_data_file_url: URL of the file containing posts data. Default is f"{script_inputs_folder_url}/tweets.parquet".
  * embedded_posts_file_url: URL of the file containing embedded posts data. Default is f"{script_inputs_folder_url}/embedded_tweets.parquet".
* Flags

  * embed_posts_captions: Set to True to embed posts' captions. Default is True.
  * calculate_optimal_clusters_number: Set to True to calculate the optimal number of clusters. Default is True.
##### Note: If these flags are set to False, the script will use the output files from previous runs. Ensure to set the flags to True for the initial run to generate the necessary output files.
