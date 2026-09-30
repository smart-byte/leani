// Mainnet beacon chain timing. Slots are 12 s and epochs 32 slots, so the
// current slot follows from the clock alone. Revisit if a fork changes the
// slot time.
export const GENESIS_SECONDS = 1606824023;
export const SLOT_SECONDS = 12;
export const SLOTS_PER_EPOCH = 32;

export function slotClock(nowMs: number) {
  const elapsed = nowMs / 1000 - GENESIS_SECONDS;
  const slot = Math.floor(elapsed / SLOT_SECONDS);
  return {
    slot,
    epoch: Math.floor(slot / SLOTS_PER_EPOCH),
    slotInEpoch: slot % SLOTS_PER_EPOCH,
    secondsIntoSlot: elapsed - slot * SLOT_SECONDS,
  };
}
