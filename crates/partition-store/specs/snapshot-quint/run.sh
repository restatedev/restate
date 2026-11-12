#!/usr/bin/env bash
# Runs every configuration x invariant of the snapshot-store model with fixed seeds, plus the
# directed runs (`quint test`) and the reachability witnesses.
# Prints exactly one PASS / FAIL / VIOLATION line per check.
# All configurations use LAT=1 (< MARGIN) except cfg_lapsing_lat4 (C8), which uses LAT=4.
#
#   ./run.sh            full run  (~15 min)
#   SAMPLES_HOLD=20000 ./run.sh   faster, weaker
set -u
Q=${QUINT:-quint}
QT=${QUINT:-quint}
cd "$(dirname "$0")" || exit 1

# sample budget: the main invariants get the full budget, the structural ones less
S_BIG=${S_BIG:-100000}
S_SMALL=${S_SMALL:-20000}
# sample budget for the reachability witnesses (they only need to be reached once)
S_WIT=${S_WIT:-300}
# witness traces are longer than the invariant traces so that 3 publishes / evictions are reachable
W_STEPS=${W_STEPS:-200}
SEED=${SEED:-11}
STEPS=${STEPS:-60}

CONFIGS=(cfg_perfect cfg_perfect_commit_err cfg_lapsing_bskew0 cfg_lapsing_bskew3
         cfg_lapsing_unb_skew0 cfg_lapsing_unb_skew3 cfg_lapsing_r1 cfg_lapsing_lat4)

# one line per (config, invariant)
for cfg in "${CONFIGS[@]}"; do
  for inv in I1 I2 I3 I4 I5; do
    if [ "$inv" = I1 ] || [ "$inv" = I3 ]; then budget=$S_BIG; else budget=$S_SMALL; fi
    out=$("$Q" run "$cfg.qnt" --init=M::init --step=M::step --invariant="M::$inv" \
          --max-steps="$STEPS" --max-samples="$budget" --seed="$SEED" --verbosity=1 2>&1)
    if printf '%s' "$out" | grep -q '^\[violation\]'; then
      echo "VIOLATION  $cfg  $inv  (seed=$SEED, <=$budget samples, $STEPS steps)"
    elif printf '%s' "$out" | grep -q '^\[ok\]'; then
      echo "PASS       $cfg  $inv  (seed=$SEED, $budget samples, $STEPS steps)"
    else
      echo "FAIL       $cfg  $inv  (seed=$SEED) -- $(printf '%s' "$out" | head -3 | tr '\n' ' ')"
    fi
  done
done

# a second seed for the two configurations where everything is expected to hold
for cfg in cfg_perfect cfg_lapsing_bskew0; do
  for inv in I1 I3; do
    out=$("$Q" run "$cfg.qnt" --init=M::init --step=M::step --invariant="M::$inv" \
          --max-steps="$STEPS" --max-samples="$S_BIG" --seed=12 --verbosity=1 2>&1)
    if printf '%s' "$out" | grep -q '^\[violation\]'; then
      echo "VIOLATION  $cfg  $inv  (seed=12, <=$S_BIG samples, $STEPS steps)"
    elif printf '%s' "$out" | grep -q '^\[ok\]'; then
      echo "PASS       $cfg  $inv  (seed=12, $S_BIG samples, $STEPS steps)"
    else
      echo "FAIL       $cfg  $inv  (seed=12) -- $(printf '%s' "$out" | head -3 | tr '\n' ' ')"
    fi
  done
done

# reachability witnesses: a witness must be reached in some state of some trace
for cfg in cfg_perfect cfg_lapsing_unb_skew3 cfg_lapsing_r1 cfg_perfect_commit_err; do
  out=$("$Q" run "$cfg.qnt" --init=M::init --step=M::step \
        --witnesses=M::W_publishes M::W_evictions M::W_discardSsts M::W_foreignPublish \
                    M::W_sweeps M::W_discardRetained \
        --max-steps="$W_STEPS" --max-samples="$S_WIT" --seed="$SEED" --verbosity=1 2>&1)
  if printf '%s' "$out" | grep -q '^\[error\]\|Runtime error\|Parsing failed'; then
    echo "FAIL       $cfg  witnesses -- $(printf '%s' "$out" | head -3 | tr '\n' ' ')"
  else
    printf 'PASS       %s  witnesses (seed=%s, %s samples, %s steps) --' "$cfg" "$SEED" "$S_WIT" "$W_STEPS"
    printf '%s' "$out" | grep 'was witnessed in' |
      sed -E 's/.*M::(W_[A-Za-z]+) was witnessed in ([0-9]+) trace.*/\1=\2/' | tr '\n' ' '
    echo
  fi
done

# directed runs: exact interleavings that must reach the violation (a test passes when the
# interleaving is executable and the asserted violation is reached)
for cfg in cfg_perfect_commit_err cfg_lapsing_r1 cfg_lapsing_bskew3 cfg_lapsing_unb_skew0 cfg_lapsing_lat4; do
  out=$("$Q" test "$cfg.qnt" 2>&1)
  while read -r name; do
    if printf '%s' "$out" | grep -q "ok $name passed"; then
      echo "PASS       $cfg  $name  (directed run)"
    else
      echo "FAIL       $cfg  $name  (directed run)"
    fi
  done < <(printf '%s' "$out" | grep -oE '[A-Za-z]+Test (passed|failed)' | sed -E 's/ (passed|failed)$//' | sort -u)
done
