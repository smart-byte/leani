// Build-time token highlighting for terminal/code blocks. Runs on trusted
// committed content only; output is injected with set:html.

export function esc(value: string): string {
  return value.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

export function hlShell(line: string): string {
  return esc(line)
    .replace(/"[^"]*"/g, (m) => `<span class="hl-str">${m}</span>`)
    .replace(/(^|\s)(--[\w-]+)/g, '$1<span class="hl-flag">$2</span>')
    .replace(/(?<![\w#.\-])(\d[\d,]*)(?![\w%])/g, '<span class="hl-num">$1</span>')
    .replace(/^\$ /, '<span class="hl-prompt">$ </span>');
}

export function hlTs(line: string): string {
  const escaped = esc(line);
  if (/^\s*\/\//.test(escaped)) return `<span class="hl-com">${escaped}</span>`;
  return escaped
    .replace(/"[^"]*"/g, (m) => `<span class="hl-str">${m}</span>`)
    // Inline comments need surrounding whitespace so protocol `//` inside
    // string literals (http://…) stays untouched.
    .replace(/(\s)(\/\/ .*)$/, '$1<span class="hl-com">$2</span>')
    .replace(/\b(import|from|const|await|for|of|async)\b/g, '<span class="hl-kw">$1</span>')
    .replace(/\b(\d[\d_]*n?)\b(?![\w-])/g, '<span class="hl-num">$1</span>');
}

export function hlRust(line: string): string {
  const escaped = esc(line);
  if (/^\s*\/\//.test(escaped)) return `<span class="hl-com">${escaped}</span>`;
  return escaped
    .replace(/"[^"]*"/g, (m) => `<span class="hl-str">${m}</span>`)
    .replace(/\b(async|await|dyn|fn|for|impl|let|pub|struct|use)\b/g, '<span class="hl-kw">$1</span>')
    .replace(/\b(\d[\d_]*)\b(?![\w-])/g, '<span class="hl-num">$1</span>');
}
