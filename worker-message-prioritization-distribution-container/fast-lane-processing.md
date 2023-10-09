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
the tag can be preceded with incrementing numbers 
eg: CAF_ADJUST_QUEUE_WEIGHT, CAF_ADJUST_QUEUE_WEIGHT_1, CAF_ADJUST_QUEUE_WEIGHT_2 and so on. 
* Inside this tag must contain a string of the regex to match the staging queue, followed by a comma, followed
by an integer that is 0 or larger. Note:
  * There **cannot be any spaces** in the entire string 
  * The number following the comma (aka the weight) **cannot be negative**
  * **Decimal numbers are allowed** in the form of 0.5 or .5
  * There cannot be a zero preceding non-decimal weight values eg: 04, or 020
* If there are multiple matches on a staging queues, the weight will be set to the longest matching string. 
For example if `"tenant1"` is set to a weight of 3, and `"enrichment-workflow"` is set to a weight of 10. 
Then the staging queue `"dataprocessing-classification-in»/tenant1/enrichment-workflow"` will be given a weight
of 10. 
* In the case of 2 matches on a staging queue with equal length string matches, the weight will be set to the 
higher weight value. 
* Setting a weight above 1 will give a staging queue more target queue capacity, and setting below 1 will give less.
For staging queues that do not have their weight altered, weight will default to 1.

### Examples of suitable tags 
* <CAF_ADJUST_QUEUE_WEIGHT>enrichment\-workflow$,10</CAF_ADJUST_QUEUE_WEIGHT> 
* <CAF_ADJUST_QUEUE_WEIGHT_1>tenant1,0</CAF_ADJUST_QUEUE_WEIGHT_1>
* <CAF_ADJUST_QUEUE_WEIGHT_2>tenant2,3</CAF_ADJUST_QUEUE_WEIGHT_2>
* <CAF_ADJUST_QUEUE_WEIGHT_3>dataprocessing-langdetect-in,7</CAF_ADJUST_QUEUE_WEIGHT_3>
* <CAF_ADJUST_QUEUE_WEIGHT_4>repository-initialization-workflow$,0.5</CAF_ADJUST_QUEUE_WEIGHT_4>
* <CAF_ADJUST_QUEUE_WEIGHT_5>tenant3,.5</CAF_ADJUST_QUEUE_WEIGHT_5>

### Examples of tags that will throw an error
* <CAF_ADJUST_QUEUE_WEIGHT>enrichment\-workflow$, 10</CAF_ADJUST_QUEUE_WEIGHT> 
* <CAF_ADJUST_QUEUE_WEIGHT_1>tenant1,03</CAF_ADJUST_QUEUE_WEIGHT_1>
* <CAF_ADJUST_QUEUE_WEIGHT_3>dataprocessing-langdetect-in,-7</CAF_ADJUST_QUEUE_WEIGHT_3>

## Distribution of unused target queue capacity
An additional feature when fast lane processing is turned on, is the ability to use the entire target queue capacity
if the demand is there. 
The increase of target queue capacity for certain queues, opens the possibility of those queues not having 
any messages but still taking up that space. When CAF_CONSUMPTION_TARGET_CALCULATOR_MODE is set to 'FastLane' any messages
not used by queues will be redistributed to queues with more messages to be processed, ensuring no wasted capacity.
