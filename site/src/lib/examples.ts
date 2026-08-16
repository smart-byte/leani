import registry from '../generated/examples.json';

export type ExampleId = keyof typeof registry.examples;
export type GeneratedExample = (typeof registry.examples)[ExampleId];

export function exampleSource(id: ExampleId): GeneratedExample {
  const example = registry.examples[id];
  if (!example) throw new Error(`unknown generated example: ${id}`);
  return example;
}
