// cfg_lapsing_unb_skew3
module cfg_lapsing_unb_skew3 {
  import snapshot_store(
    N = 2, F = 4, NUM_RETAINED = 2, MODE = 1, MAX_SKEW = 3, BOUNDED = false, MAX_STALL = 1, ALLOW_COMMIT_ERR = false, ALLOW_CRASH = false, LAT = 1
  ) as M from "./snapshot_store"
}
