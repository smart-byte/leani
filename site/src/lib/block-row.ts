// Tokenizes one `leani subscribe blocks` pretty row for the landing terminal.
// Runs on committed, recorded rows only; output is injected as HTML. Fields
// with inner spaces stay unbreakable so narrow terminals wrap between fields.
import { esc } from './highlight';

const FINALITY = /^(preview|included|finalized)( undo)?$/;

function field(text: string): string {
  const finality = text.match(FINALITY);
  if (finality) {
    const undo = finality[2] ? ' <span class="br-undo">undo</span>' : '';
    return `<span class="br-fin br-${finality[1]}">${finality[1]}</span>${undo}`;
  }
  if (/^\d{4}-\d\d-\d\dT[\d:.]+Z$/.test(text)) return `<span class="br-ts">${esc(text)}</span>`;
  const pair = text.match(/^([a-z_]+=)(.*)$/);
  if (!pair) return esc(text);
  const value = esc(pair[2] ?? '').replace(/ (gwei)$/, ' <span class="br-unit">$1</span>');
  return `<span class="br-key">${pair[1]}</span>${value}`;
}

export function blockRowHtml(row: string): string {
  return row
    .split('  ')
    .map((text) => (text.includes(' ') ? `<span class="br-f">${field(text)}</span>` : field(text)))
    .join('  ');
}
