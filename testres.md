Test 2A

jingxuanbao@Jingxuans-MBP raft % go test -run 2A
Test (2A): initial election ...
  ... Passed --   3.0  3   46    5676    0
Test (2A): election after network failure ...
  ... Passed --   4.5  3  102    8916    0
Test (2A): multiple elections ...
  ... Passed --   6.3  7  678   58700    0
PASS
ok      6.5840/raft     14.068s

Test 2B

(base) jingxuanbao@Jingxuans-MBP raft % go test -run 2B
Test (2B): basic agreement ...
  ... Passed --   1.1  3   16    4326    3
Test (2B): RPC byte count ...
  ... Passed --   3.1  3   48  113746   11
Test (2B): test progressive failure of followers ...
  ... Passed --   5.1  3  108   22518    3
Test (2B): test failure of leaders ...
  ... Passed --   5.5  3  168   36980    3
Test (2B): agreement after follower reconnects ...
  ... Passed --   6.5  3  112   28688    7
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   3.8  5  180   35456    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.8  3   12    3250    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   6.6  3  164   40588    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  31.2  5 2216 1647832  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.3  3   36   10122   12
PASS
ok      6.5840/raft     66.139s


Test 2C

(base) jingxuanbao@Jingxuans-MBP raft % go test -run 2C
Test (2C): basic persistence ...
  ... Passed --   4.6  3  244   46308    6
Test (2C): more persistence ...
  ... Passed --  20.4  5 2544  341600   17
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   2.3  3   54   11416    4
Test (2C): Figure 8 ...
  ... Passed --  39.0  5 31456 7395228   26
Test (2C): unreliable agreement ...
  ... Passed --   6.8  5  224   73279  246
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  42.3  5 3372 7901390  370
Test (2C): churn ...
  ... Passed --  16.2  5  764  358354  337
Test (2C): unreliable churn ...
  ... Passed --  16.2  5 1676  830640  122
PASS
ok      6.5840/raft     148.108s