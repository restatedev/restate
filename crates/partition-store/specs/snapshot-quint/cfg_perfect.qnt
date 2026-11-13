// cfg_perfect
module cfg_perfect {
  import snapshot_store(
    N = 2, F = 4, NUM_RETAINED = 2, MODE = 0, MAX_SKEW = 0, BOUNDED = true, MAX_STALL = 1, ALLOW_COMMIT_ERR = false, ALLOW_CRASH = false, LAT = 1
  ) as M from "./snapshot_store"
}
