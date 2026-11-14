# SPEC rows

One SPEC row = one row issue = one branch `row/<issue>-<row>` = one lane session = one PR (handbook `software/conventions/sdlc.md` §2b). Every open issue sits under its row issue or is listed below. Rows are in [docs/SPEC.md](../docs/SPEC.md) order.

| Row | Row issue | Status | Folded issues | Open PRs |
|---|---|---|---|---|
| multi-group-host | #1110 | shipped | 5 | #1092, #1062, #1061, #1017, #924, #919, #908, #903, #900, #899, #898, #894, #887, #885, #826, #784 |
| raft-consensus | #1111 | shipped | 3 | #1077, #994, #993, #928, #927, #914, #865 |
| read-index-readiness | #1112 | shipped | 1 | #1031 |
| membership-changes | #1113 | shipped | 1 | #984, #949, #923, #916, #897, #893, #880, #876, #870 |
| quic-transport | #1114 | shipped | 6 | #1100, #1098, #1091, #1090, #1076, #1063, #1060, #1050, #1049, #918, #905, #904, #888 |
| wire-protocol | #1115 | shipped | 1 | #1099, #911, #906 |
| log-storage | #1116 | shipped | 8 | #1059, #1047, #1032, #1027, #1020, #1014, #1000, #988, #940, #907, #901, #875 |
| snapshots | #1117 | shipped | 7 | #1101, #1083, #1082, #1078, #1075, #1034, #1021, #995, #926, #915, #896, #890 |
| state-machines | #1118 | shipped | 3 | #1093, #1086, #1045, #990, #892, #884, #874 |
| client-sessions | #1119 | shipped | 1 | #1102, #986, #917, #886 |
| write-batching | #1120 | shipped | 1 | #1079, #921, #910, #873 |
| execute-on-leader | #1121 | shipped | 0 | #895 |
| seal-barrier | #1122 | shipped | 8 | #1097, #1080, #1048, #1016, #991, #987, #982, #980, #925, #909, #883, #882 |
| shamir-primitive | #1123 | shipped | 1 | #1046, #891 |
| memory-enclave | #1124 | shipped | 0 | #920 |
| observability | #1125 | shipped | 1 | #1033, #922, #913 |
| contrib-dragonboat | #1126 | shipped | 11 | #1081, #1074, #1044, #1043, #1019, #1018, #999, #998, #997, #996, #985, #981, #964, #939, #889, #881, #879, #878, #877, #872 |

Rows with no open work: `lease-read`, `heartbeat-under-fsync`, `0-rtt-policy`, `fips-builds`, `pq-key-exchange`, `shard-pause`, `compatibility`, `testing`, `release`.
