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

* The environment variable tag **must** begin with CAF_ADJUST_QUEUE_WEIGHT. If there is more than one weight to be set, 
the tag can be preceded with incrementing numbers 
eg: CAF_ADJUST_QUEUE_WEIGHT, CAF_ADJUST_QUEUE_WEIGHT_1, CAF_ADJUST_QUEUE_WEIGHT_2 and so on. 
* Inside this tag must contain a string of the regex to match the staging queue, followed by a comma, followed
by a double that is 0 or larger. Note:
  * There **cannot be any spaces** in the entire string 
  * The number following the comma (aka the weight) **cannot be negative**
  * **Decimal numbers are allowed** in the form of 0.5 or .5
  * There cannot be a zero preceding non-decimal weight values eg: 04, or 020
* If there are multiple matches on a staging queues, the weight will be set to the longest matching string. 
  For example if `"dataprocessing-classification-in»/tenant1"` is set to a weight of 20, and `"enrichment-workflow"` 
  is set to a weight of 3. Then the staging queue `"dataprocessing-classification-in»/tenant1/enrichment-workflow"` 
  will be given a weight of 20. This ensures all of tenant1's messages on dataprocessing-classification-in will be 
  weighted at the higher value of 20. This has been implemented to allow altering of tenants' weight to take
  precedence over altering of workflow weight. This allows certain tenants to be given more or less target queue capacity for quicker 
  or slower processing when required. Hence, why these strings should be more specific. 
* The more of the staging queue string matched by the regex pattern, the more specific the weight adjustment will be. 
* In the case of 2 matches on a staging queue with equal length string matches, the weight will be set to the 
  higher weight value.
* A staging queue with a weight of 2 will have twice the amount of messages moved to a target queue than a staging queue with 
  weighting of 1. Alternately a staging queue with weight 0.5 will have half the amount of messages moved to a target queue than a 
  staging queue with weighting of 1. 
* Setting the weight to 0 will stop the movement of any messages off staging queues which match the regex in the environment variable. 
  The messages will not get lost, but will be held on the staging queues until that weight changes back to a positive double, at which 
  point the messages will begin to move again and get processed as normal. 

### Examples of suitable tags 
* Regex to match enrichment-workflow at the end of a staging queue string, followed by weight of 10 with no spaces. 
  * <CAF_ADJUST_QUEUE_WEIGHT>enrichment\-workflow$,10</CAF_ADJUST_QUEUE_WEIGHT> 
* Regex to match tenant1 staging queues for dataprocessing-classification-in, followed by weight of 0 with no spaces.
  * <CAF_ADJUST_QUEUE_WEIGHT_1>dataprocessing-classification-in»/tenant1,0</CAF_ADJUST_QUEUE_WEIGHT_1>
* Regex to match tenant2 staging queues for bulk-indexer-in, followed by weight of 3 with no spaces.
  * <CAF_ADJUST_QUEUE_WEIGHT_2>bulk-indexer-in»/tenant2,3</CAF_ADJUST_QUEUE_WEIGHT_2>
* Regex to match tenant3 staging queues for dataprocessing-langdetect-in, followed by weight of 7 with no spaces.
  * <CAF_ADJUST_QUEUE_WEIGHT_3>dataprocessing-langdetect-in»/tenant2,7</CAF_ADJUST_QUEUE_WEIGHT_3>
* Regex to match repository-initialization-workflow$ at the end of a staging queue string, followed by weight of 0.5 with no spaces.
  * <CAF_ADJUST_QUEUE_WEIGHT_4>repository-initialization-workflow$,0.5</CAF_ADJUST_QUEUE_WEIGHT_4>
* Regex to match tenant3 anywhere in staging queue string, followed by weight of .5 with no spaces.
  * <CAF_ADJUST_QUEUE_WEIGHT_5>tenant3,.5</CAF_ADJUST_QUEUE_WEIGHT_5>

### Examples of tags that will throw an error
* Regex to match enrichment-workflow at the end of a staging queue string, followed by a **space** before weight 10. The space will 
  throw an error.
  * <CAF_ADJUST_QUEUE_WEIGHT>enrichment\-workflow$, 10</CAF_ADJUST_QUEUE_WEIGHT> 
* Regex to match tenant1 staging queues for dataprocessing-classification-in, followed by a **zero** before weight 3. 
  The zero will throw an error.
  * <CAF_ADJUST_QUEUE_WEIGHT_1>dataprocessing-classification-in»/tenant1,03</CAF_ADJUST_QUEUE_WEIGHT_1>
* Regex to match tenant3 staging queues for dataprocessing-langdetect-in, followed by a **negative** weight. 
  The negative weight will throw an error.
  * <CAF_ADJUST_QUEUE_WEIGHT_3>dataprocessing-langdetect-in»/tenant3,-7</CAF_ADJUST_QUEUE_WEIGHT_3>

## Distribution of unused target queue capacity
An additional feature when fast lane processing is turned on, is the ability to use the entire target queue capacity
if the demand is there. 
The increase of target queue capacity for certain queues, opens the possibility of those queues not having 
any messages but still taking up that space. When CAF_CONSUMPTION_TARGET_CALCULATOR_MODE is set to 'FastLane' any messages
not used by queues will be redistributed to queues with more messages to be processed, ensuring no wasted capacity.
