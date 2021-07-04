# DataEngineerChallenge


## Overview

This document describes the solution to https://github.com/Pay-Baymax/DataEngineerChallenge.
This repo will only show the Spark solution.


## Solution (streaming)

I added a new streaming solution with a few more assumptions to solve this challenge in a realtime context.
But I believe the batch approach below is a better approach to take given the format and form of the sample data.

The streaming solution is under [realtime](./realtime) folder in this repo with a Flink application code with full explanation of the design and implementation.


## Solution (batch)

### Understand the input data
This is my first step. I need to look into the data to find out the  data size, schema and so on.
So, what I did is to use **Jupyter** to inspect the input data that I uploaded to the hdfs.

From the inspectation, what I learned:
- Potential duration of a session can be extremely long. (up to 11 hours)
- Normal traffic per hour can be around 100k in this data set, with peak traffic around 300k.
- Most sessions will likely end around 20 minutes. And there are plenty of sessions end after 15 minutes.
- There are 15 hours in the data.

For this inspectation of the data, please refer to the [inspectation notebook](./doc/Data%20Inspect/Data%20Inspect.md) under the `doc` folder.

### Design Consideration

#### Should I use streaming?

Probably, yes. If we only think about the mission, it makes perfect sense to process the traffic data in the streaming application.

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

***Since 1.1.0***: I'm no longer union the accesses in the previous hour, but put a only single record per user as the
watermark of the uncut session for the next hour. Please refer to [SessionCutWatermark](./src/main/scala/visualskyrim/schema/SessionCutWatermark.scala).

#### How should the output look like

According to the [Analytical goals](https://github.com/Pay-Baymax/DataEngineerChallenge#processing--analytical-goals),
all interested metrics are on the **session** instead of individual access.
With that being said, it would make more sense to me to output session with these metrics directly instead of outputting the accesses with a session id attached to them.
This benefits us with:
- Easier and faster to calculate duration, session number and average accesses per session, since they are already aggregated at session level.
- Avoid confusion about "*If a session last for two hours, and then we check the session number for each hour, should this session count as 1 session on each hour?*"

Other that that, we will also output the pending accesses that are not yet being cut into a session. This result will be used as the input for next hour's batch.

#### Traffic is very unstable

Noticed that the traffic changes quite a lot from hour to hour. I'm not sure if this reflects the real traffic trend, but if so, might need to consider dynamic allocation to make job to get required resource easily.


## Sessionize Result

The result is shown in the the [Jupyter notebook](./doc/Result%20Check.md) under the `doc` folder.
The result data I inspected is generated by Spark application running on YARN. And output is saved on HDFS.

But the same result can be found the [data](./data) folder in this project.
- The `sessionized` folder is the result for sessions partitioned by hour.
- The `pending` folder is the session watermark used for processing session in the next hour.
- The `error` folder is the parse exception folder, which is supposed to be empty.

## How to run this application on local

First, you need to use the [application.conf.template](./src/main/resources/application.conf.template) to create your own `src/main/resources/application.conf`.
Here is the example:

```
data {
  input: "file:///Users/kong.mu/work/DataEngineerChallenge/data"
  sessionized: "file:///Users/kong.mu/work/DataEngineerChallenge/data/sessionized"
  pending: "file:///Users/kong.mu/work/DataEngineerChallenge/data/pending"

  error: "file:///Users/kong.mu/work/DataEngineerChallenge/data/error"
}

session {
  timeout: 1200
  maxDuration: 21600 //6 * 60 * 60
}
```

Then build the project:

```bash
sbt clean assembly
```

Since the first hour in this data set is `2015-07-22T02`, you should start with that hour:

> Use the `--firstHour` flag to specify the first hour of this pipeline. (and skip reading pending watermark)

```bash
./bin/spark-submit --class visualskyrim.Sessionize  target/scala-2.11/sessionize-assembly-1.1.0.jar --hour 2015-07-22T02 --firstHour
```

Then for the rest of the hours:
```bash
./bin/spark-submit --class visualskyrim.Sessionize  target/scala-2.11/sessionize-assembly-1.1.0.jar --hour 2015-07-22TXX
```
