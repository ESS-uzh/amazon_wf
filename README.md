## Welcome to amazon_wf: An app to collect and process sentinel2 tiles over the brazilian legal amazon border.

### Note: This app is mean to be used by the members of the ESS group only.

### Description:
- The tiles are grouped in 17 batches of ~25 tiles each.

- The app's backend defines different actions. 
  Each action operates on a chosen batch (1 to 17). Those are:
  * update
  * download
  * correct
  * stack
  * biodiv_pca
  * biodiv_out

- The app's frontend displays an overview of the project; i.e. the status/processing_level 
   and the lcation of each tile. It lets users select the tile to be visually inspected; i.e. those at 'pca' processing 
   level and update their processing level; to 'pca_ready'. 
