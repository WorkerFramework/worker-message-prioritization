# Fast lane processing 

## Altering staging queue weight
When CAF_CONSUMPTION_TARGET_CALCULATOR_MODE is set to 'FastLane' the ability exists to 
increase or decrease the weight of certain staging queues. Doing this will give these 
staging queues more or less capacity of the target queue depending on the weight.

### Method of weighting
Staging queues hold messages waiting to be moved onto their target queue for processing. 
Target queues have a set length, therefore only have a certain capacity to hold messages. 
Weighting allows queues to be set so that they are given more or less of this available capacity and therefore will
get processed quicker or slower than the other staging queues.

How the staging queue capacity is calculated: 

$$ Target \ queue \ capacity \ given \ to \ staging \ queue  = {{Total \ target \ queue \ capacity \over Sum \ of \ weights \ of \ all 
\ staging \ queues} * Weight \ of \ staging \ queue.} $$

### Format to follow when setting staging queue weights: 

* The environment variable name **must** begin with CAF_ADJUST_QUEUE_WEIGHT. If there is more than one staging queue regular expression and weight to be set, 
the environment variable name can be appended with _ and incrementing numbers 
eg: CAF_ADJUST_QUEUE_WEIGHT, CAF_ADJUST_QUEUE_WEIGHT_1, CAF_ADJUST_QUEUE_WEIGHT_2 and so on. 
* The value of this environment variable must contain a string of the regex to match the staging queue, followed by a comma, followed
by a double that is 0 or larger. Note:
  * There **cannot be any spaces** in the entire string 
  * The number following the comma (aka the weight) **cannot be negative**
  * **Decimal numbers are allowed** in the form of 0.5 or .5
  * There **cannot be a zero preceding** non-decimal weight values eg: 04, or 020
* If there are multiple matches on a staging queue, the weight will be set to the **longest** matching string. This allows 
  certain tenant weights to be given precedence over any other set weights. This allows faster or slower processing 
  to be given to certain tenants when required. 
  For example: tenant1 weight is set with the regex: `".*/tenant1/.*"` to a weight of 20. This regex indicates we 
  want to match everything that comes before and after "/tenant1/" using expression `.*`. This will match the entire length 
  of **any** tenant1 staging queue. 
  If at the same time `"enrichment-workflow"` is set to a weight of 3, then the staging queue 
  `"dataprocessing-classification-in»/tenant1/enrichment-workflow"` will be set to a weight of 20 because tenant1 regex 
  matches the whole string, whereas the workflow only matches for the end of the string.
* In the case of 2 matches on a staging queue with equal length string matches eg: two weights are matching to the whole string,
  the weight will be set to the higher weight value.
* A staging queue with a weight of 2 will have twice the amount of messages moved to a target queue than a staging queue with 
  weighting of 1. Alternately a staging queue with weight 0.5 will have half the amount of messages moved to a target queue than a 
  staging queue with weighting of 1. 
* Setting the weight to 0 will stop the movement of any messages off staging queues which match the regex in the environment variable. 
  The messages will not get lost, but will be held on the staging queues until that weight changes back to a positive double, at which 
  point the messages will begin to move again and get processed as normal. 

### CAF_ADJUST_QUEUE_WEIGHT Examples
* Regex to match enrichment-workflow at the end of a staging queue string, followed by weight of 10 with no spaces. Provided
  there are no other weights set to match more of an enrichment-workflow staging queue, weight will be set to 10.
  * `<CAF_ADJUST_QUEUE_WEIGHT>enrichment\-workflow$,10</CAF_ADJUST_QUEUE_WEIGHT>`
* Regex to match whole string for tenant1 staging queues, followed by weight of 0 with no spaces. Matching the whole string will 
  allow the tenant weight to take precedence over other weight adjustments. 
  * `<CAF_ADJUST_QUEUE_WEIGHT_1>.*/tenant1/.*,0</CAF_ADJUST_QUEUE_WEIGHT_1>`
* Regex to match repository-initialization-workflow$ at the end of a staging queue string, followed by a decimal weight of 0.5 with no 
  spaces. If there are no other weights set to match more of a repository-initialization-workflow staging queue, weight will be set to 
  0.5.
  * `<CAF_ADJUST_QUEUE_WEIGHT_4>repository-initialization-workflow$,0.5</CAF_ADJUST_QUEUE_WEIGHT_4>`
* Regex to match tenant3 staging queues for bulk-indexer-in, followed by a decimal weight of .5 with no spaces. Provided
  there are no other weights set to match more of a tenant3 job on bulk-indexer-in staging queue, weight will be set to .5. 
  Note this is not matching the entire queue, therefore it is not guaranteed this tenant2 match will take precedence over others. 
  * `<CAF_ADJUST_QUEUE_WEIGHT_5>bulk-indexer-in»/tenant2,.5</CAF_ADJUST_QUEUE_WEIGHT_5>`

### Invalid CAF_ADJUST_QUEUE_WEIGHT Examples
* Regex to match enrichment-workflow at the end of a staging queue string, followed by a **space** before weight 10. The space will 
  throw an error.
  * `<CAF_ADJUST_QUEUE_WEIGHT>enrichment\-workflow$, 10</CAF_ADJUST_QUEUE_WEIGHT>`
* Regex to match tenant1 staging queues for dataprocessing-classification-in, followed by a **zero** before weight 3. 
  The zero will throw an error.
  * `<CAF_ADJUST_QUEUE_WEIGHT_1>dataprocessing-classification-in»/tenant1,03</CAF_ADJUST_QUEUE_WEIGHT_1>`
* Regex to match tenant3 staging queues for dataprocessing-langdetect-in, followed by a **negative** weight. 
  The negative weight will throw an error.
  * `<CAF_ADJUST_QUEUE_WEIGHT_3>dataprocessing-langdetect-in»/tenant3,-7</CAF_ADJUST_QUEUE_WEIGHT_3>`

## Distribution of unused target queue capacity
An additional feature when fast lane processing is turned on, is the ability to use the entire target queue capacity
if the demand is there. 
The increase of target queue capacity for certain queues, opens the possibility of those queues not having 
any messages but still taking up that space. When CAF_CONSUMPTION_TARGET_CALCULATOR_MODE is set to 'FastLane' any messages
not used by queues will be redistributed to queues with more messages to be processed, ensuring no wasted capacity.
