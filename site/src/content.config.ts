import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';
import { z } from 'astro/zod';
import { docsSchema } from '@astrojs/starlight/schema';
import { DOC_SECTIONS, PUBLIC_DOC_PATTERNS, docIdFromEntry } from './lib/docs';

const docs = defineCollection({
  loader: glob({
    base: new URL('../../docs/', import.meta.url),
    pattern: [...PUBLIC_DOC_PATTERNS],
    generateId: ({ entry }) => docIdFromEntry(entry),
  }),
  schema: (context) =>
    docsSchema({
      extend: z.object({
        section: z.enum(DOC_SECTIONS),
        order: z.number().int().nonnegative(),
        audience: z
          .array(z.enum(['app-developer', 'operator', 'processor-author', 'contributor']))
          .optional()
          .default([]),
        status: z.enum(['preview', 'stable', 'deprecated']).optional().default('stable'),
        generated: z.boolean().optional().default(false),
      }),
    })(context).transform((data) => ({
      ...data,
      sidebar: { ...data.sidebar, order: data.order },
    })),
});

export const collections = { docs };
