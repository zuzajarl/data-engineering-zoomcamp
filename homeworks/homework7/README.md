# Question 1

Command run: 
homework7-redpanda-1

Output: 
rpk version: v25.3.9
Git ref:     836b4a36ef6d5121edbb1e68f0f673c2a8a244e2
Build date:  2026 Feb 26 07 47 54 Thu
OS/Arch:     linux/arm64
Go version:  go1.24.3

Redpanda Cluster
  node-1  v25.3.9 - 836b4a36ef6d5121edbb1e68f0f673c2a8a244e2

Answer: 
v25.3.9

# Question 2
To get the answer I updated the code in [producer](src/producers/producer.py) and [model](src/models.py).
The upload lasted 3 seconds, so the clostes answer is 10.

Answer: 
10s

# Question 3
To get the answer I updated the code in [](src/consumers/consumer.py)
Answer:
8506

# Question 4
The code is available in [](src/job/count_job.py)
Answer:
74

# Question 5
The code is available in [](src/job/longest_streak_job.py)
I also needed to create appropriate table in postgres and then query it: 

CREATE TABLE session_events_streak (
    session_start TIMESTAMP NOT NULL,
    session_end TIMESTAMP NOT NULL,
    PULocationID INT NOT NULL,
    num_trips BIGINT NOT NULL,
    PRIMARY KEY (session_start, session_end, PULocationID)
);

SELECT PULocationID, num_trips 
FROM session_events_streak 
ORDER BY num_trips DESC 
LIMIT 1;

Answer:
81

# Question 6
The code is available in [](src/job/largest_tip_job.py)
In postgres: 
DROP TABLE IF EXISTS window_tip_amount;
CREATE TABLE window_tip_amount (
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    tip_amount DOUBLE PRECISION,
    PRIMARY KEY (window_start, window_end)
);
SELECT window_start, window_end, tip_amount 
FROM window_tip_amount 
ORDER BY tip_amount DESC 
LIMIT 1;

Answer:
2025-10-16 18:00:00