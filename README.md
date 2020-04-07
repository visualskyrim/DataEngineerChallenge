# DataEngineerChallenge


## Overview

This document describes the solution to https://github.com/Pay-Baymax/DataEngineerChallenge.
This repo will only show the Spark solution.


## Solution
### Understand the input data
This is my first step. I need to look into the data to find out the  data size, schema and so on.
So, what I did is to use **Jupyter** to inspect the input data that I uploaded to the hdfs.

From the inspectation, what I learned:
- Potential duration of a session can be extremely long. (up to 11 hours)
- Normal traffic per hour can be around 100k in this data set, with peak traffic around 300k.
- Most sessions will likely end around 20 minutes. And there are plenty of sessions end after 15 minutes.
- There are 15 hours in the data.

For this inspectation of the data, please refer to the [inspectation notebook](./doc/Data%20Inspect/Data%20Inspect.md).

### Design Consideration

#### Should I use streaming?

Absolutely, yes. If we only think about the mission, it makes perfect sense to process the traffic data in the streaming application.
Normally I would set up logstash to stream the access log from AWS to a kafka topic, then build a streaming application to provide the realtime analysis.
However, given the form of the data is actually a packed file, I assume that the scenario is more of a batched context.
That's why I chose to use Spark to build a batched application.


#### How about the granularity of the batch

Based on the requirement, it would make less sense to calculate the session of the first hour of the day at the beginning of the next day in a daily batch.
Why don't show it in the next hour with an hourly batch?
Besides, the timestamp in the data is in UTC, thus introducing a concept of "day" would be very confusing.

Another thing we can benefit from using a hourly batch is that, we can potentially reduce the cluster cost by using less resource to process hourly data instead of daily data.

#### How should we deal with the sessions not ended within one hour?

Since we need to calculate the sessions in the next hour, and a session can theoretically last forever,
we need two things:
- Concat the accesses from last hour that are not in any ended session, with the accesses in the current hour.
- A limitation for how long at most a session can last.

For the second one, we need it because if some sessions last too long, we will have serious data skew problem.

#### How should the output look like

According to the [Analytical goals](https://github.com/Pay-Baymax/DataEngineerChallenge#processing--analytical-goals),
all interested metrics are on the **session** instead of individual access.
With that being said, it would make more sense to me to output session with these metrics directly instead of outputting the accesses with a session id attached to them.
This benefits us with:
- Easier and faster to calculate duration, session number and average accesses per session, since they are already aggregated at session level.
- Avoid confusion about "*If a session last for two hours, and then we check the session number for each hour, should this session count as 1 session on each hour?*"

Other that that, we will also output the pending accesses that are not yet being cut into a session. This result will be used as the input for next hour's batch. 