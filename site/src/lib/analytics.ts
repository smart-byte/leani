// Self-hosted, cookieless Umami. The privacy policy describes exactly this
// setup: counting only on leani.dev, honouring Do Not Track, and dropping
// query strings and fragments from recorded URLs.
const WEBSITE_ID = '4e47ebb5-7a6f-4ead-903d-e07bbef13716';

export const UMAMI_ORIGIN = 'https://analytics.smart-byte.com';

/** Attributes for the tracker's script tag; undefined when no website ID is set. */
export const umamiScript = WEBSITE_ID
  ? {
      src: `${UMAMI_ORIGIN}/script.js`,
      'data-website-id': WEBSITE_ID,
      'data-domains': 'leani.dev',
      'data-do-not-track': 'true',
      'data-exclude-search': 'true',
      'data-exclude-hash': 'true',
    }
  : undefined;
