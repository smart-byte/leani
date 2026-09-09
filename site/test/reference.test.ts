import { describe, expect, test } from 'bun:test';
import cli from '../src/generated/cli-reference.json';
import config from '../src/generated/config-reference.json';
import http from '../src/generated/http-reference.json';
import processors from '../src/generated/processors-reference.json';
import sdk from '../src/generated/sdk-reference.json';

describe('generated contract reference', () => {
  test('covers every routed native API path and the administrative job APIs', () => {
    expect(http.operations.length).toBeGreaterThanOrEqual(60);
    const paths = new Set(http.operations.map((operation) => operation.path));
    for (const path of [
      '/v1/processors/{processor}/streams/live/consumers/{consumer}/stream',
      '/admin/v1/backfill-subscriptions',
      '/admin/v1/materialization-jobs',
      '/admin/v1/raw-history-jobs',
      '/v1/network/status',
    ]) {
      expect(paths.has(path)).toBe(true);
    }
  });

  test('extracts broad configuration and SDK contracts', () => {
    expect(config.fields.length).toBeGreaterThanOrEqual(150);
    expect(config.fields.some((field) => field.path === 'processors[].coverage.verification_segment_blocks')).toBe(true);
    const members = new Set(sdk.methods.map((member) => member.name));
    expect(members.has('processors.queryAndFollow')).toBe(true);
    expect(members.has('backfill.subscribe')).toBe(true);
    expect(sdk.types.some((type) => type.name === 'ChangeEnvelope')).toBe(true);
    expect(members.has('processors.consumers.acknowledge')).toBe(true);
  });

  test('consumes the tracked Rust-owned fixtures', () => {
    expect(cli.command.name).toBe('leani');
    expect(cli.command.subcommands.some((command) => command.name === 'doctor')).toBe(true);
    expect(processors.processors).toHaveLength(7);
  });
});
