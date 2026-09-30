import { expect, test } from 'bun:test';
import { slotClock } from '../src/lib/beacon';

test('the slot clock lands on real Mainnet fork activations', () => {
  // Deneb activated at epoch 269568 and Altair at epoch 74240, each on the
  // first slot of its epoch at the published UTC time.
  expect(slotClock(Date.parse('2024-03-13T13:55:35Z'))).toEqual({
    slot: 269568 * 32,
    epoch: 269568,
    slotInEpoch: 0,
    secondsIntoSlot: 0,
  });
  const beforeAltair = slotClock(Date.parse('2021-10-27T10:56:23Z') - 1);
  expect(beforeAltair.epoch).toBe(74239);
  expect(beforeAltair.slotInEpoch).toBe(31);
  expect(beforeAltair.secondsIntoSlot).toBeCloseTo(11.999, 3);
});
